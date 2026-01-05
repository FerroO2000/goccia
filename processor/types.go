// Package processor contains the processor stages.
// All the processor stages take a message from a previous stage,
// through an input connector, and produce a message for the next stage,
// through an output connector.
package processor

import (
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
)

type msgBody = message.Body

type msgBodyPtr[T any] interface {
	*T
	msgBody
}

type msg[T msgBody] = message.Message[T]

type msgSer = message.Serializable

type msgConn[T msgBody] = connector.Connector[*msg[T]]

type cfg = config.Config
type stageCfg = config.WithStage
