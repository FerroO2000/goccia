package connector

import (
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/rb"
)

type msgBody = message.Body

type msgWrap[T msgBody] = message.Message[T]

// RingBuffer is a lock-free spsc generic ring buffer.
type RingBuffer[T any] = rb.RingBuffer[T]
