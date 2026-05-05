package stage

import (
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
)

type msgBody = message.Body

type msgConn[T msgBody] = connector.MessageConnector[T]
