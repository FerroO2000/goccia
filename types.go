package goccia

import (
	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
)

// StageRunningMode represents the running mode of a stage.
type StageRunningMode = config.StageRunningMode

const (
	// StageRunningModeSingle enforces a single-threaded running mode.
	StageRunningModeSingle = config.StageRunningModeSingle
	// StageRunningModePool enforces a multi-threaded running mode (worker pool).
	StageRunningModePool = config.StageRunningModePool
)

// StageConfig represents the configuration for a stage.
type StageConfig = config.Stage

type Stage = stage.Stage

// Connector represents the interface for a generic connector
// to be used for connecting the stages.
type Connector[T any] = connector.Connector[T]

// MessageEnvelope is an untyped alias for a generic message envelope.
type MessageEnvelope[T message.Body] = message.Message[T]

// NewMessageEnvelope returns a new message envelope.
func NewMessageEnvelope[T message.Body](body T) *MessageEnvelope[T] {
	return message.NewMessage(body)
}
