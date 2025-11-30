package processor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	csvBaseTimestamp = "2025-11-26T15:30:00Z"
	csvBaseInt       = 10
	csvBaseDouble    = 0.5
)

const csvBaseFile = `2025-11-26T15:30:00Z,10,0.5,true,string
2025-11-26T16:30:00Z,20,1.5,false,string-
`

var csvFiles = struct {
	clean               string
	cleanWithHeader     string
	cleanWithoutNewline string

	emptyColumns      string // missing timestamp, double and string
	invalidColumnData string
	missingColumns    string // missing string column
}{
	clean:               csvBaseFile,
	cleanWithHeader:     "timestamp,int,double,bool,string\n" + csvBaseFile,
	cleanWithoutNewline: csvBaseFile[:len(csvBaseFile)-1],

	emptyColumns:      `,10,,true,`,
	invalidColumnData: `timestamp,int,double,bool,`,
	missingColumns:    `2025-11-26T15:30:00Z,10,0.5,true`,
}

var csvDecoderConfigTest = &csvDecoderConfig{
	columns: []*CSVColumnDef{
		{
			Name:            "timestamp",
			Type:            CSVColumnTypeTimestamp,
			TimestampLayout: time.RFC3339,
		},
		{
			Name: "int",
			Type: CSVColumnTypeInt,
		},
		{
			Name: "double",
			Type: CSVColumnTypeFloat,
		},
		{
			Name: "bool",
			Type: CSVColumnTypeBool,
		},
		{
			Name: "string",
			Type: CSVColumnTypeString,
		},
	},
}

func csvDecoderCheckRow(assert *assert.Assertions, row []*CSVColumn, rowIdx int, validity []bool) {
	assert.Len(row, len(csvDecoderConfigTest.columns))

	timestampValue, err := time.Parse(time.RFC3339, csvBaseTimestamp)
	assert.NoError(err)
	intValue := csvBaseInt
	doubleValue := csvBaseDouble
	boolValue := true
	stringValue := "string"

	for range rowIdx {
		timestampValue = timestampValue.Add(time.Hour)
		intValue += 10
		doubleValue += 1.0
		boolValue = !boolValue
		stringValue = stringValue + "-"
	}

	for i, col := range row {
		expectedColDef := csvDecoderConfigTest.columns[i]
		assert.Equal(expectedColDef.Name, col.Name)
		assert.Equal(expectedColDef.Type, col.Type)

		assert.Equal(validity[i], col.IsDataValid)

		if !col.IsDataValid {
			continue
		}

		switch expectedColDef.Type {
		case CSVColumnTypeTimestamp:
			assert.Equal(timestampValue, col.TimestampValue)

		case CSVColumnTypeInt:
			assert.Equal(intValue, int(col.IntValue))

		case CSVColumnTypeFloat:
			assert.Equal(doubleValue, col.FloatValue)

		case CSVColumnTypeBool:
			assert.Equal(boolValue, col.BoolValue)

		case CSVColumnTypeString:
			assert.Equal(stringValue, col.StringValue)
		}
	}
}

func csvDecoderCleanTest(t *testing.T, decoder *csvDecoder) {
	assert := assert.New(t)

	msg := &CSVMessage{}
	err := decoder.decode([]byte(csvFiles.clean), msg)
	assert.NoError(err)

	rows := msg.Rows
	assert.Len(rows, 2)

	for rowIdx, row := range rows {
		csvDecoderCheckRow(assert, row, rowIdx, []bool{true, true, true, true, true})
	}
}

func csvDecoderCleanWithHeaderTest(t *testing.T, decoder *csvDecoder) {
	assert := assert.New(t)

	msg := &CSVMessage{}
	err := decoder.decode([]byte(csvFiles.cleanWithHeader), msg)
	assert.NoError(err)

	rows := msg.Rows
	assert.Len(rows, 3)

	for rowIdx, row := range rows {
		if rowIdx == 0 {
			// Header row should contain invalid columns data
			csvDecoderCheckRow(assert, row, rowIdx, []bool{false, false, false, false, true})
			continue
		}

		csvDecoderCheckRow(assert, row, rowIdx-1, []bool{true, true, true, true, true})
	}
}

func csvDecoderCleanWithoutNewlineTest(t *testing.T, decoder *csvDecoder) {
	assert := assert.New(t)

	msg := &CSVMessage{}
	err := decoder.decode([]byte(csvFiles.cleanWithoutNewline), msg)
	assert.NoError(err)

	rows := msg.Rows
	assert.Len(rows, 2)

	for rowIdx, row := range rows {
		csvDecoderCheckRow(assert, row, rowIdx, []bool{true, true, true, true, true})
	}
}

func csvDecoderEmptyColumnsTest(t *testing.T, decoder *csvDecoder) {
	assert := assert.New(t)

	msg := &CSVMessage{}
	err := decoder.decode([]byte(csvFiles.emptyColumns), msg)
	assert.NoError(err)

	rows := msg.Rows
	assert.Len(rows, 1)

	csvDecoderCheckRow(assert, rows[0], 0, []bool{false, true, false, true, false})
}

func csvDecoderInvalidColumnDataTest(t *testing.T, decoder *csvDecoder) {
	assert := assert.New(t)

	msg := &CSVMessage{}
	err := decoder.decode([]byte(csvFiles.invalidColumnData), msg)
	assert.NoError(err)

	rows := msg.Rows
	assert.Len(rows, 1)

	csvDecoderCheckRow(assert, rows[0], 0, []bool{false, false, false, false, false})
}

func csvDecoderMissingColumnsTest(t *testing.T, decoder *csvDecoder) {
	assert := assert.New(t)

	msg := &CSVMessage{}
	err := decoder.decode([]byte(csvFiles.missingColumns), msg)
	assert.Error(err)
}

func Test_csvDecoder(t *testing.T) {
	decoder := newCSVDecoder(csvDecoderConfigTest)

	t.Run("clean", func(t *testing.T) {
		csvDecoderCleanTest(t, decoder)
	})

	t.Run("clean_with_header", func(t *testing.T) {
		csvDecoderCleanWithHeaderTest(t, decoder)
	})

	t.Run("clean_without_newline", func(t *testing.T) {
		csvDecoderCleanWithoutNewlineTest(t, decoder)
	})

	t.Run("empty_columns", func(t *testing.T) {
		csvDecoderEmptyColumnsTest(t, decoder)
	})

	t.Run("invalid_column_data", func(t *testing.T) {
		csvDecoderInvalidColumnDataTest(t, decoder)
	})

	t.Run("missing_columns", func(t *testing.T) {
		csvDecoderMissingColumnsTest(t, decoder)
	})
}

func Benchmark_csvDecoder(b *testing.B) {
	b.ReportAllocs()

	decoder := newCSVDecoder(csvDecoderConfigTest)

	data := []byte("2025-11-26T15:30:00Z,10,0.5,true,string\n")
	msg := &CSVMessage{}

	for b.Loop() {
		_ = decoder.decode(data, msg)
	}
}
