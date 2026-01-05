package processor

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/pool"
)

///////////////
//  MESSAGE  //
///////////////

var _ msgSer = (*CSVEncodedMessage)(nil)

// CSVEncodedMessage represents a CSV encoded message.
type CSVEncodedMessage struct {
	Data []byte
}

func newCSVEncodedMessage(data []byte) *CSVEncodedMessage {
	return &CSVEncodedMessage{
		Data: data,
	}
}

// Destroy cleans up the message.
func (m *CSVEncodedMessage) Destroy() {}

// GetBytes returns the encoded bytes of a CSV message.
func (m *CSVEncodedMessage) GetBytes() []byte {
	return m.Data
}

///////////////
//  ENCODER  //
///////////////

type csvEncoderConfig struct {
	columns []*CSVColumnDef
}

type csvEncoder struct {
	columnDefs  []*CSVColumnDef
	columnCount int
}

func newCSVEncoder(config *csvEncoderConfig) *csvEncoder {
	return &csvEncoder{
		columnDefs:  config.columns,
		columnCount: len(config.columns),
	}
}

func (e *csvEncoder) encode(rows [][]*CSVColumn) []byte {
	sb := &strings.Builder{}

	sb.Grow(len(rows) * e.columnCount * 16)

	for _, row := range rows {
		colCount := len(row)

		for colIdx := range e.columnCount {
			if colIdx > 0 {
				sb.WriteByte(',')
			}

			if colIdx >= colCount {
				e.encodeDefaultColumn(sb, colIdx)
				continue
			}

			e.encodeColumn(sb, row[colIdx], colIdx)
		}

		sb.WriteByte('\n')
	}

	return []byte(sb.String())
}

func (e *csvEncoder) encodeColumn(sb *strings.Builder, col *CSVColumn, currCol int) {
	if !col.IsDataValid {
		e.encodeDefaultColumn(sb, currCol)
		return
	}

	colDef := e.columnDefs[currCol]

	switch colDef.Type {
	case CSVColumnTypeString:
		sb.WriteString(col.StringValue)

	case CSVColumnTypeInt:
		sb.WriteString(strconv.FormatInt(col.IntValue, 10))

	case CSVColumnTypeFloat:
		sb.WriteString(strconv.FormatFloat(col.FloatValue, 'f', -1, 64))

	case CSVColumnTypeBool:
		sb.WriteString(strconv.FormatBool(col.BoolValue))

	case CSVColumnTypeTimestamp:
		sb.WriteString(col.TimestampValue.Format(colDef.getTimestampLayout()))
	}
}

func (e *csvEncoder) encodeDefaultColumn(sb *strings.Builder, currCol int) {
	colDef := e.columnDefs[currCol]

	switch colDef.Type {
	case CSVColumnTypeString:
		sb.WriteString("")

	case CSVColumnTypeInt:
		sb.WriteString("0")

	case CSVColumnTypeFloat:
		sb.WriteString("0.0")

	case CSVColumnTypeBool:
		sb.WriteString("false")

	case CSVColumnTypeTimestamp:
		sb.WriteString(time.Now().Format(colDef.getTimestampLayout()))
	}
}

//////////////
//  WORKER  //
//////////////

type csvEncoderWorkerArgs struct {
	encoder *csvEncoder
}

func newCSVEncoderWorkerArgs(encoder *csvEncoder) *csvEncoderWorkerArgs {
	return &csvEncoderWorkerArgs{
		encoder: encoder,
	}
}

type csvEncoderWorker struct {
	pool.BaseWorker

	encoder *csvEncoder
}

func newCSVEncoderWorkerInstMaker() workerInstanceMaker[*csvEncoderWorkerArgs, *CSVMessage, *CSVEncodedMessage] {
	return func() workerInstance[*csvEncoderWorkerArgs, *CSVMessage, *CSVEncodedMessage] {
		return &csvEncoderWorker{}
	}
}

func (w *csvEncoderWorker) Init(_ context.Context, args *csvEncoderWorkerArgs) error {
	w.encoder = args.encoder
	return nil
}

func (w *csvEncoderWorker) Handle(ctx context.Context, msgIn *msg[*CSVMessage]) (*msg[*CSVEncodedMessage], error) {
	_, span := w.Tel.NewTrace(ctx, "encode csv data")
	defer span.End()

	rows := msgIn.GetBody().Rows
	data := w.encoder.encode(rows)

	csvEncMsg := newCSVEncodedMessage(data)
	msgOut := message.NewMessage(csvEncMsg)

	msgOut.SaveSpan(span)

	return msgOut, nil
}

func (w *csvEncoderWorker) Close(_ context.Context) error {
	return nil
}

/////////////
//  STAGE  //
/////////////

// CSVEncoderStage is a processor stage that encodes CSV messages.
type CSVEncoderStage struct {
	stage[*csvEncoderWorkerArgs, *CSVMessage, *CSVEncodedMessage, *CSVConfig]
}

// NewCSVEncoderStage returns a new CSV encoder stage.
func NewCSVEncoderStage(
	inputConnector msgConn[*CSVMessage], outputConnector msgConn[*CSVEncodedMessage], cfg *CSVConfig,
) *CSVEncoderStage {
	return &CSVEncoderStage{
		stage: newStage("csv_encoder", inputConnector, outputConnector, newCSVEncoderWorkerInstMaker(), cfg),
	}
}

// Init initializes the stage.
func (s *CSVEncoderStage) Init(ctx context.Context) error {
	encoder := newCSVEncoder(&csvEncoderConfig{
		columns: s.Config().Columns,
	})

	return s.stage.Init(ctx, newCSVEncoderWorkerArgs(encoder))
}
