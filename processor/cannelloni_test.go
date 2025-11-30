package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type cannelloniTD struct {
	frame       *cannelloniFrame
	encodedData []byte
}

var cannelloniTestData = cannelloniTD{
	frame: &cannelloniFrame{version: 1,
		opCode:         1,
		sequenceNumber: 128,
		messageCount:   2,
		messages: []cannelloniFrameMessage{
			{canID: 1, dataLen: 2, data: []byte{0b11000001, 0b11000001}},
			// Message with can fd flags
			{canID: 0x0100, canFDFlags: 1, dataLen: 2, data: []byte{0b11000001, 0b11000001}},
		},
	},

	encodedData: []byte{
		// Header
		0x01, 0x01, 0x80,
		0, 0x02, // Message count

		// First message
		0, 0, 0, 0x01, // 4 bytes can-id
		0x02, // Data len without can-fd flags
		0b11000001,
		0b11000001,

		// Second message
		0, 0, 0x01, 0, // 4 bytes can-id
		0x82, 1, // Data len with can-fd flags (add | 0x80)
		0b11000001,
		0b11000001,
	},
}

func Test_cannelloniEncoder(t *testing.T) {
	assert := assert.New(t)

	encoder := newCannelloniEncoder()

	res := encoder.encode(cannelloniTestData.frame)
	assert.Equal(cannelloniTestData.encodedData, res)
}

func Test_cannelloniDecoder(t *testing.T) {
	assert := assert.New(t)

	decoder := newCannelloniDecoder()

	res, err := decoder.decode(cannelloniTestData.encodedData)
	assert.NoError(err)
	assert.Equal(cannelloniTestData.frame, res)
}

func Benchmark_cannelloniDecoder(b *testing.B) {
	b.ReportAllocs()

	decoder := newCannelloniDecoder()
	for b.Loop() {
		_, err := decoder.decode(cannelloniTestData.encodedData)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_cannelloniEncoder(b *testing.B) {
	b.ReportAllocs()

	encoder := newCannelloniEncoder()
	for b.Loop() {
		_ = encoder.encode(cannelloniTestData.frame)
	}
}
