package processor

import (
	"sync"
	"time"

	stageCommon "github.com/FerroO2000/goccia/internal/stage"
)

// CSVColumnType represents the type of a CSV column.
type CSVColumnType uint8

const (
	// CSVColumnTypeString represents a column with string values.
	CSVColumnTypeString CSVColumnType = iota
	// CSVColumnTypeInt represents a column with integer values.
	CSVColumnTypeInt
	// CSVColumnTypeFloat represents a column with float values.
	CSVColumnTypeFloat
	// CSVColumnTypeBool represents a column with boolean values.
	CSVColumnTypeBool
	// CSVColumnTypeTimestamp represents a column with timestamp values.
	CSVColumnTypeTimestamp
)

//////////////
//  CONFIG  //
//////////////

// CSVColumnDef represents the definition of a CSV column.
type CSVColumnDef struct {
	// Name is the name of the column.
	Name string

	// Type is the type of the column.
	Type CSVColumnType

	// TimestampLayout is the layout used to parse the timestamp column.
	// If empty, the RFC3339 layout is used.
	TimestampLayout string
}

// NewCSVColumnDef returns a new csv column definition.
func NewCSVColumnDef(name string, valueType CSVColumnType) *CSVColumnDef {
	return &CSVColumnDef{
		Name: name,
		Type: valueType,
	}
}

func (cd *CSVColumnDef) getTimestampLayout() string {
	if cd.TimestampLayout != "" {
		return cd.TimestampLayout
	}

	return time.RFC3339
}

// CSVConfig represents the configuration of the CSV decoder/encoder stages.
type CSVConfig struct {
	Stage *stageCommon.Config

	// Columns is the list of column definitions.
	Columns []*CSVColumnDef
}

// DefaultCSVConfig returns the default configuration for the CSV decoder/encoder stages.
// It returns an empty list of column definitions;
// use the provided methods to add column definitions.
func DefaultCSVConfig(runningMode stageCommon.RunningMode) *CSVConfig {
	return &CSVConfig{
		Stage:   stageCommon.DefaultConfig(runningMode),
		Columns: []*CSVColumnDef{},
	}
}

// AddColumnDef adds a column definition to the list of definitions.
func (c *CSVConfig) AddColumnDef(colDef *CSVColumnDef) {
	c.Columns = append(c.Columns, colDef)
}

///////////////
//  MESSAGE  //
///////////////

// CSVColumn represents a column entry in a CSV message.
type CSVColumn struct {
	// Name is the name of the column.
	Name string

	// Type is the type of the column.
	Type CSVColumnType

	// IsDataValid states whether the data in the column is valid.
	IsDataValid bool

	// StringValue is the value of the column if it is a string.
	StringValue string

	// IntValue is the value of the column if it is an integer.
	IntValue int64

	// FloatValue is the value of the column if it is a float.
	FloatValue float64

	// BoolValue is the value of the column if it is a boolean.
	BoolValue bool

	// TimestampValue is the value of the column if it is a timestamp.
	TimestampValue time.Time
}

func newCSVColumn(name string, typ CSVColumnType) *CSVColumn {
	return &CSVColumn{
		Name:        name,
		Type:        typ,
		IsDataValid: true,
	}
}

// NewCSVStringColumn returns a new string column.
func NewCSVStringColumn(name string, value string) *CSVColumn {
	col := newCSVColumn(name, CSVColumnTypeString)
	col.StringValue = value
	return col
}

// NewCSVIntColumn returns a new integer column.
func NewCSVIntColumn(name string, value int64) *CSVColumn {
	col := newCSVColumn(name, CSVColumnTypeInt)
	col.IntValue = value
	return col
}

// NewCSVFloatColumn returns a new float column.
func NewCSVFloatColumn(name string, value float64) *CSVColumn {
	col := newCSVColumn(name, CSVColumnTypeFloat)
	col.FloatValue = value
	return col
}

// NewCSVBoolColumn returns a new boolean column.
func NewCSVBoolColumn(name string, value bool) *CSVColumn {
	col := newCSVColumn(name, CSVColumnTypeBool)
	col.BoolValue = value
	return col
}

// NewCSVTimestampColumn returns a new timestamp column.
func NewCSVTimestampColumn(name string, value time.Time) *CSVColumn {
	col := newCSVColumn(name, CSVColumnTypeTimestamp)
	col.TimestampValue = value
	return col
}

var _ msgEnv = (*CSVMessage)(nil)

var csvMessagePool = sync.Pool{
	New: func() any {
		return &CSVMessage{
			RowCount: 0,
			Rows:     make([][]*CSVColumn, 0, 256),
		}
	},
}

// CSVMessage represents a CSV message containing a list of rows.
type CSVMessage struct {
	// RowCount is the number of rows in the message.
	RowCount int

	// Rows is the list of rows in the message.
	// Each row consists of a list of columns.
	Rows [][]*CSVColumn
}

// NewCSVMessage returns a new CSV message.
// It returns a message from the pool.
func NewCSVMessage() *CSVMessage {
	return csvMessagePool.Get().(*CSVMessage)
}

// Destroy cleans up the message and returns it to the pool.
func (m *CSVMessage) Destroy() {
	m.RowCount = 0
	m.Rows = m.Rows[:0]
	csvMessagePool.Put(m)
}

// AddRow adds a row to the message.
func (m *CSVMessage) AddRow(row []*CSVColumn) {
	m.RowCount++
	m.Rows = append(m.Rows, row)
}
