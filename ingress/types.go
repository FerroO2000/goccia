// Package ingress contains the ingress stages.
package ingress

import (
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
)

type msgBody = message.Body

type msg[T msgBody] = message.Message[T]

type msgSer = message.Serializable

type msgConn[T msgBody] = connector.Connector[*msg[T]]

type cfg = config.Config
