// Package egress contains the egress stages.
package egress

import (
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
)

type msgEnv = message.Envelope

type msg[T msgEnv] = message.Message[T]

type msgSer = message.Serializable

type msgConn[T msgEnv] = connector.Connector[*msg[T]]

type cfg = config.Config
type stageCfg = config.WithStage
