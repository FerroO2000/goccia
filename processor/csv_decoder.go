package processor

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/pool"
)

///////////////
//  DECODER  //
///////////////

type csvDecoderConfig struct {
	columns []*CSVColumnDef
}

type csvDecoder struct {
	columnDefs  []*CSVColumnDef
	columnCount int
}

func newCSVDecoder(config *csvDecoderConfig) *csvDecoder {
	return &csvDecoder{
		columnDefs:  config.columns,
		columnCount: len(config.columns),
	}
}

func (d *csvDecoder) decode(data []byte, msg *CSVMessage) error {
	acc := &strings.Builder{}
	acc.Grow(16)

	rowBuf := make([]*CSVColumn, d.columnCount)
	columnIdx := 0

	idx := 0
	for idx < len(data) {
		// Check if we have a multi-byte rune
		if data[idx] >= utf8.RuneSelf {
			r, size := utf8.DecodeRune(data[idx:])
			idx += size
			acc.WriteRune(r)
			continue
		}

		b := data[idx]
		idx++

		switch b {
		case ',', '\n':
			// Check if it is an empty row
			if b == '\n' && columnIdx == 0 {
				continue
			}

			// Found a column
			col := d.decodeColumn(acc.String(), columnIdx)

			rowBuf[columnIdx] = col
			columnIdx++

			acc.Reset()

			// Reached end of row
			if columnIdx == d.columnCount {
				// Check if new line symbol is valid.
				// Consider that the last row may not end with a new line.
				if b != '\n' && idx < len(data) {
					// Invalid CSV format, expected new line
					return errors.New("invalid CSV format: expected new line")
				}

				columnIdx = 0

				// Copy row buffer to avoid an allocation on each row
				row := make([]*CSVColumn, d.columnCount)
				copy(row, rowBuf)
				msg.Rows = append(msg.Rows, row)
			}

		case '\r':
			// Ignore carriage return
			continue

		default:
			acc.WriteByte(b)
		}
	}

	// Check if the last column is missing new line
	if columnIdx == d.columnCount-1 {
		// Decode last column
		col := d.decodeColumn(acc.String(), columnIdx)

		rowBuf[columnIdx] = col
		columnIdx = 0

		msg.Rows = append(msg.Rows, rowBuf)
	}

	// Check if the last row is incomplete
	if columnIdx != 0 {
		return errors.New("invalid CSV format: incomplete row at the end")
	}

	return nil
}

func (d *csvDecoder) decodeColumn(columnData string, columnIdx int) *CSVColumn {
	column := &CSVColumn{
		Name:        d.columnDefs[columnIdx].Name,
		Type:        d.columnDefs[columnIdx].Type,
		IsDataValid: true,
	}

	switch column.Type {
	case CSVColumnTypeString:
		d.decodeString(column, columnData)

	case CSVColumnTypeInt:
		d.decodeInt(column, columnData)

	case CSVColumnTypeFloat:
		d.decodeFloat(column, columnData)

	case CSVColumnTypeBool:
		d.decodeBool(column, columnData)

	case CSVColumnTypeTimestamp:
		d.decodeTimestamp(column, columnData, columnIdx)
	}

	return column
}

func (d *csvDecoder) decodeString(column *CSVColumn, data string) {
	if len(data) == 0 {
		column.IsDataValid = false
		return
	}

	column.StringValue = data
}

func (d *csvDecoder) decodeInt(column *CSVColumn, data string) {
	intVal, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		column.IsDataValid = false
		return
	}

	column.IntValue = intVal
}

func (d *csvDecoder) decodeFloat(column *CSVColumn, data string) {
	floatVal, err := strconv.ParseFloat(data, 64)
	if err != nil {
		column.IsDataValid = false
		return
	}

	column.FloatValue = floatVal
}

func (d *csvDecoder) decodeBool(column *CSVColumn, data string) {
	boolVal, err := strconv.ParseBool(data)
	if err != nil {
		column.IsDataValid = false
		return
	}

	column.BoolValue = boolVal
}

func (d *csvDecoder) decodeTimestamp(column *CSVColumn, data string, currColumn int) {
	timeValue, err := time.Parse(d.columnDefs[currColumn].getTimestampLayout(), data)
	if err != nil {
		column.IsDataValid = false
		return
	}

	column.TimestampValue = timeValue
}

//////////////
//  WORKER  //
//////////////

type csvDecoderWorkerArgs struct {
	decoder *csvDecoder
}

func newCSVDecoderWorkerArgs(decoder *csvDecoder) *csvDecoderWorkerArgs {
	return &csvDecoderWorkerArgs{
		decoder: decoder,
	}
}

type csvDecoderWorker[T msgSer] struct {
	pool.BaseWorker

	decoder *csvDecoder
}

func newCSVDecoderWorkerInstMaker[T msgSer]() workerInstanceMaker[*csvDecoderWorkerArgs, T, *CSVMessage] {
	return func() workerInstance[*csvDecoderWorkerArgs, T, *CSVMessage] {
		return &csvDecoderWorker[T]{}
	}
}

func (w *csvDecoderWorker[T]) Init(_ context.Context, args *csvDecoderWorkerArgs) error {
	w.decoder = args.decoder
	return nil
}

func (w *csvDecoderWorker[T]) Handle(ctx context.Context, msgIn *msg[T]) (*msg[*CSVMessage], error) {
	_, span := w.Tel.NewTrace(ctx, "decode csv data")
	defer span.End()

	data := msgIn.GetEnvelope().GetBytes()
	csvMsg := NewCSVMessage()

	if err := w.decoder.decode(data, csvMsg); err != nil {
		csvMsg.Destroy()

		w.Tel.LogError("failed to decode CSV data", err)
		return nil, err
	}

	msgOut := message.NewMessage(csvMsg)

	msgOut.SaveSpan(span)

	return msgOut, nil
}

func (w *csvDecoderWorker[T]) Close(_ context.Context) error {
	return nil
}

/////////////
//  STAGE  //
/////////////

// CSVDecoderStage is a processor stage that decodes raw chunks of CSV data.
type CSVDecoderStage[T msgSer] struct {
	stage[*csvDecoderWorkerArgs, T, *CSVMessage, *CSVConfig]
}

// NewCSVDecoderStage returns a new CSV decoder stage.
func NewCSVDecoderStage[T msgSer](
	inputConnector msgConn[T], outputConnector msgConn[*CSVMessage], cfg *CSVConfig,
) *CSVDecoderStage[T] {

	return &CSVDecoderStage[T]{
		stage: newStage("csv_decoder", inputConnector, outputConnector, newCSVDecoderWorkerInstMaker[T](), cfg),
	}
}

// Init initializes the stage.
func (s *CSVDecoderStage[T]) Init(ctx context.Context) error {
	decoder := newCSVDecoder(&csvDecoderConfig{
		columns: s.Config().Columns,
	})

	return s.stage.Init(ctx, newCSVDecoderWorkerArgs(decoder))
}
