package egress

import (
	"context"
	"iter"
	"math/big"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	qdb "github.com/questdb/go-questdb-client/v3"
	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/pool"
	stageCommon "github.com/FerroO2000/goccia/internal/stage"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

// QuestDBConfig structs contains the configuration for the QuestDB egress stage.
type QuestDBConfig struct {
	Stage *stageCommon.Config

	// Address of the QuestDB server.
	//
	// Default: "localhost:9000"
	Address string
}

// DefaultQuestDBConfig returns the default configuration for the QuestDB egress stage.
func DefaultQuestDBConfig(runningMode stageCommon.RunningMode) *QuestDBConfig {
	return &QuestDBConfig{
		Stage:   stageCommon.DefaultConfig(runningMode),
		Address: "localhost:9000",
	}
}

///////////////
//  MESSAGE  //
///////////////

var _ msgEnv = (*QuestDBMessage)(nil)

// QuestDBMessage represents a QuestDB message.
// It contains the definition of the rows and columns to be inserted
// into the database.
type QuestDBMessage struct {
	rows []*QuestDBRow
}

// Destroy cleans up the message.
func (qm *QuestDBMessage) Destroy() {}

// AddRow adds a row to the message.
func (qm *QuestDBMessage) AddRow(row *QuestDBRow) {
	qm.rows = append(qm.rows, row)
}

// AddRows adds multiple rows to the message.
func (qm *QuestDBMessage) AddRows(rows ...*QuestDBRow) {
	if len(qm.rows) == 0 {
		qm.rows = rows
		return
	}

	qm.rows = append(qm.rows, rows...)
}

// GetRows returns the rows of the message.
func (qm *QuestDBMessage) GetRows() []*QuestDBRow {
	return qm.rows
}

func (qm *QuestDBMessage) iterRows() iter.Seq[*QuestDBRow] {
	return slices.Values(qm.rows)
}

// QuestDBColumnType represents the type of a column.
// It does not include the symbol column since it is defined
// as a stand-alone struct in the row.
type QuestDBColumnType int

const (
	// QuestDBColumnTypeBool defines a boolean column.
	QuestDBColumnTypeBool QuestDBColumnType = iota
	// QuestDBColumnTypeInt defines an integer column.
	QuestDBColumnTypeInt
	// QuestDBColumnTypeLong defines a long integer column.
	QuestDBColumnTypeLong
	// QuestDBColumnTypeFloat defines a float column.
	QuestDBColumnTypeFloat
	// QuestDBColumnTypeString defines a string column.
	QuestDBColumnTypeString
	// QuestDBColumnTypeTimestamp defines a timestamp column.
	QuestDBColumnTypeTimestamp
)

// QuestDBColumn represents a column of a row.
type QuestDBColumn struct {
	name  string
	typ   QuestDBColumnType
	value any
}

func newQuestDBColumn(name string, typ QuestDBColumnType, value any) QuestDBColumn {
	return QuestDBColumn{
		name:  name,
		typ:   typ,
		value: value,
	}
}

// NewQuestDBBoolColumn returns a new boolean column.
func NewQuestDBBoolColumn(name string, value bool) QuestDBColumn {
	return newQuestDBColumn(name, QuestDBColumnTypeBool, value)
}

// NewQuestDBIntColumn returns a new integer column.
func NewQuestDBIntColumn(name string, value int64) QuestDBColumn {
	return newQuestDBColumn(name, QuestDBColumnTypeInt, value)
}

// NewQuestDBLongColumn returns a new long integer column.
func NewQuestDBLongColumn(name string, value *big.Int) QuestDBColumn {
	return newQuestDBColumn(name, QuestDBColumnTypeLong, value)
}

// NewQuestDBFloatColumn returns a new float column.
func NewQuestDBFloatColumn(name string, value float64) QuestDBColumn {
	return newQuestDBColumn(name, QuestDBColumnTypeFloat, value)
}

// NewQuestDBStringColumn returns a new string column.
func NewQuestDBStringColumn(name string, value string) QuestDBColumn {
	return newQuestDBColumn(name, QuestDBColumnTypeString, value)
}

// NewQuestDBTimestampColumn returns a new timestamp column.
func NewQuestDBTimestampColumn(name string, value time.Time) QuestDBColumn {
	return newQuestDBColumn(name, QuestDBColumnTypeTimestamp, value)
}

// GetName returns the name of the column.
func (qc QuestDBColumn) GetName() string {
	return qc.name
}

// GetType returns the type of the column.
func (qc QuestDBColumn) GetType() QuestDBColumnType {
	return qc.typ
}

// GetValue returns the value of the column.
func (qc QuestDBColumn) GetValue() any {
	return qc.value
}

// QuestDBSymbol represents a symbol column.
// It is defined as a stand-alone struct because a symbol column
// must be inserted before any other column.
type QuestDBSymbol struct {
	name  string
	value string
}

// NewQuestDBSymbol returns a new symbol.
func NewQuestDBSymbol(name string, value string) QuestDBSymbol {
	return QuestDBSymbol{
		name:  name,
		value: value,
	}
}

// GetName returns the name of the symbol.
func (qs QuestDBSymbol) GetName() string {
	return qs.name
}

// GetValue returns the value of the symbol.
func (qs QuestDBSymbol) GetValue() string {
	return qs.value
}

// QuestDBRow represents a row to be inserted into the database.
type QuestDBRow struct {
	table   string
	symbols []QuestDBSymbol
	columns []QuestDBColumn
}

// NewQuestDBRow returns a new row.
func NewQuestDBRow(table string) *QuestDBRow {
	return &QuestDBRow{
		table: table,
	}
}

// AddSymbol adds a symbol to the row.
func (qr *QuestDBRow) AddSymbol(symbol QuestDBSymbol) {
	qr.symbols = append(qr.symbols, symbol)
}

// AddSymbols adds multiple symbols to the row.
func (qr *QuestDBRow) AddSymbols(symbols ...QuestDBSymbol) {
	if len(qr.symbols) == 0 {
		qr.symbols = symbols
		return
	}

	qr.symbols = append(qr.symbols, symbols...)
}

// AddColumn adds a column to the row.
func (qr *QuestDBRow) AddColumn(column QuestDBColumn) {
	qr.columns = append(qr.columns, column)
}

// AddColumns adds multiple columns to the row.
func (qr *QuestDBRow) AddColumns(columns ...QuestDBColumn) {
	if len(qr.columns) == 0 {
		qr.columns = columns
		return
	}

	qr.columns = append(qr.columns, columns...)
}

////////////////////////
//  WORKER ARGUMENTS  //
////////////////////////

type questDBWorkerArgs struct {
	senderPool *qdb.LineSenderPool
}

func newQuestDBWorkerArgs(senderPool *qdb.LineSenderPool) *questDBWorkerArgs {
	return &questDBWorkerArgs{senderPool: senderPool}
}

//////////////////////
//  WORKER METRICS  //
//////////////////////

type questDBWorkerMetrics struct {
	once sync.Once

	insertedRows atomic.Int64
}

var questDBWorkerMetricsInst = &questDBWorkerMetrics{}

func (qwm *questDBWorkerMetrics) init(tel *internal.Telemetry) {
	qwm.once.Do(func() {
		qwm.initMetrics(tel)
	})
}

func (qwm *questDBWorkerMetrics) initMetrics(tel *internal.Telemetry) {
	tel.NewCounter("inserted_rows", func() int64 { return qwm.insertedRows.Load() })
}

func (qwm *questDBWorkerMetrics) addInsertedRows(amount int) {
	qwm.insertedRows.Add(int64(amount))
}

/////////////////////////////
//  WORKER IMPLEMENTATION  //
/////////////////////////////

type questDBWorker struct {
	pool.BaseWorker

	sender qdb.LineSender

	metrics *questDBWorkerMetrics
}

func newQuestDBWorkerInstMaker() workerInstanceMaker[*questDBWorkerArgs, *QuestDBMessage] {
	return func() workerInstance[*questDBWorkerArgs, *QuestDBMessage] {
		return &questDBWorker{
			metrics: questDBWorkerMetricsInst,
		}
	}
}

func (qw *questDBWorker) Init(ctx context.Context, args *questDBWorkerArgs) error {
	// Get and set the sender from the pool
	sender, err := args.senderPool.Sender(ctx)
	if err != nil {
		return err
	}
	qw.sender = sender

	// Initialize the metrics
	qw.metrics.init(qw.Tel)

	return nil
}

func (qw *questDBWorker) Deliver(ctx context.Context, msgIn *msg[*QuestDBMessage]) error {
	ctx, span := qw.Tel.NewTrace(ctx, "deliver QuestDB rows")
	defer span.End()

	qdbMsg := msgIn.GetEnvelope()
	timestamp := msgIn.GetTimestamp()

	tmpInsRows := 0
	for row := range qdbMsg.iterRows() {
		query := qw.sender.Table(row.table)

		for _, symbol := range row.symbols {
			query.Symbol(symbol.name, symbol.value)
		}

		for _, col := range row.columns {
			switch col.typ {
			case QuestDBColumnTypeBool:
				query.BoolColumn(col.name, col.value.(bool))
			case QuestDBColumnTypeInt:
				query.Int64Column(col.name, col.value.(int64))
			case QuestDBColumnTypeLong:
				query.Long256Column(col.name, col.value.(*big.Int))
			case QuestDBColumnTypeFloat:
				query.Float64Column(col.name, col.value.(float64))
			case QuestDBColumnTypeString:
				query.StringColumn(col.name, col.value.(string))
			case QuestDBColumnTypeTimestamp:
				query.TimestampColumn(col.name, col.value.(time.Time))
			}
		}

		if err := query.At(ctx, timestamp); err != nil {
			return err
		}

		tmpInsRows++
	}

	span.SetAttributes(attribute.Int64("inserted_rows", int64(tmpInsRows)))

	// Update metrics
	qw.metrics.addInsertedRows(tmpInsRows)

	return nil
}

func (qw *questDBWorker) Close(ctx context.Context) error {
	// Close the sender
	select {
	case <-ctx.Done():
		return qw.sender.Close(context.Background())
	default:
		return qw.sender.Close(ctx)
	}
}

/////////////
//  STAGE  //
/////////////

// QuestDBStage is an egress stage that writes messages to QuestDB.
type QuestDBStage struct {
	stage[*questDBWorkerArgs, *QuestDBMessage]

	cfg *QuestDBConfig

	senderPool *qdb.LineSenderPool
}

// NewQuestDBStage returns a new QuestDB egress stage.
func NewQuestDBStage(inputConnector msgConn[*QuestDBMessage], cfg *QuestDBConfig) *QuestDBStage {
	return &QuestDBStage{
		stage: newStage("questdb", inputConnector, newQuestDBWorkerInstMaker(), cfg.Stage),

		cfg: cfg,
	}
}

// Init initializes the stage.
func (qs *QuestDBStage) Init(ctx context.Context) error {
	// Create the sender pool
	senderPool, err := qdb.PoolFromOptions(
		qdb.WithAddress(qs.cfg.Address),
		qdb.WithHttp(),
		qdb.WithAutoFlushRows(75_000),
		qdb.WithRetryTimeout(time.Second),
	)
	if err != nil {
		return err
	}
	qs.senderPool = senderPool

	return qs.stage.Init(ctx, newQuestDBWorkerArgs(senderPool))
}

// Close closes the stage.
func (qs *QuestDBStage) Close() {
	qs.stage.Close()

	// Close the sender pool
	if err := qs.senderPool.Close(context.Background()); err != nil {
		qs.Tel().LogError("failed to close sender pool", err)
	}
}
