// Package worker is an internal package that contains the worker interface definitions,
// and the runner implementation.
package worker

import (
	"context"

	"github.com/FerroO2000/goccia/internal/telemetry"
)

// Worker interface defines the common methods
// for all the different kind of workers.
type Worker[Args any] interface {
	// SetTelemetry sets the telemetry for the worker.
	SetTelemetry(tel *telemetry.Telemetry)

	// Init initializes the worker with the worker's specific arguments.
	// The arguments are used to inject dependencies into the worker
	// (e.g. a database connection).
	Init(ctx context.Context, args Args) error

	// Close closes the worker.
	Close(ctx context.Context) error
}

// Processor interface defines the methods
// that a processor worker must implement.
type Processor[Args any, In, Out msgBody] interface {
	Worker[Args]

	// Handle is the processor's specific method.
	// It processes the input message and returns the output message.
	Handle(ctx context.Context, msgIn *msg[In]) (*msg[Out], error)
}

// Egress interface defines the methods
// that an egress worker must implement.
type Egress[Args any, In msgBody] interface {
	Worker[Args]

	// Deliver is the egress worker's specific method.
	// It takes the input message and it shall send the message
	// to the egress destination.
	Deliver(ctx context.Context, task *msg[In]) error
}
