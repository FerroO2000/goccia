// Package processor contains the processor stages.
// All the processor stages take a message from a previous stage,
// through an input connector, and produce a message for the next stage,
// through an output connector.
package processor

import (
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
)

type msgEnv = message.Envelope

type msgEnvPtr[T any] interface {
	*T
	msgEnv
}

type msg[T msgEnv] = message.Message[T]

type msgSer = message.Serializable

type msgConn[T msgEnv] = connector.Connector[*msg[T]]
