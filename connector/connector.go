package connector

import "time"

type Connector[T any] interface {
	Write(item T) error
	Read() (T, error)
	Close()
	SetReadTimeout(readTimeout time.Duration)
}
