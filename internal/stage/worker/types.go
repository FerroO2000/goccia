package worker

import (
	"github.com/FerroO2000/goccia/internal/message"
)

type msgBody = message.Body
type msg[T msgBody] = message.Message[T]
