package main

import (
	"context"
	"math/rand/v2"
	"sync/atomic"

	"github.com/FerroO2000/goccia/ingress"
	"github.com/FerroO2000/goccia/processor"
)

type tickerToCannelloniHandler struct {
	processor.CustomHandlerBase

	sequenceNumber atomic.Uint32
}

func newTickerToCannelloniHandler() *tickerToCannelloniHandler {
	return &tickerToCannelloniHandler{}
}

func (h *tickerToCannelloniHandler) Init(_ context.Context) error {
	return nil
}

func (h *tickerToCannelloniHandler) Handle(_ context.Context, tickerMsg *ingress.TickerMessage, cannelloniMsg *processor.CannelloniMessage) error {
	// Set the sequence number based on the tick number
	seqNum := uint8(tickerMsg.TickNumber % 256)
	cannelloniMsg.SetSequenceNumber(seqNum)

	// Get random signal data
	intVal := uint8(rand.Int32N(255))
	enumVal := uint8(rand.Int32N(3))

	// Build 2 can messages:
	// - one with the random int value repeated 8 times
	// - one with the random enum value repeated 8 times
	intMsg := processor.CANRawMessage{
		CANID:   1000,
		DataLen: 8,
		RawData: []byte{intVal, intVal, intVal, intVal, intVal, intVal, intVal, intVal},
	}

	enumMsg := processor.CANRawMessage{
		CANID:   2000,
		DataLen: 4,
		RawData: []byte{enumVal, enumVal, enumVal, enumVal},
	}

	// Add 20 messages with the same sequence number,
	// 10 are int messages and 10 are enum messages
	for range 10 {
		cannelloniMsg.AddMessage(intMsg)
		cannelloniMsg.AddMessage(enumMsg)
	}

	return nil
}

func (h *tickerToCannelloniHandler) Close() {}
