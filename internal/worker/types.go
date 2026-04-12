package worker

import "github.com/FerroO2000/goccia/internal/message"

type msg[T msgBody] = message.Message[T]

type msgBody = message.Body
