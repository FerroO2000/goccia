// Package worker is an internal package that contains the worker interface definitions,
// and the runner implementation.
package worker

import (
	"context"

	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// Worker interface defines the common methods
// for all the different kind of workers.
type Worker[Env env.Env] interface {
	SetEnvironment(env Env)

	// Init initializes the worker.
	Init(ctx context.Context) error

	// Close closes the worker.
	Close(ctx context.Context) error
}

// Processor interface defines the methods
// that a processor worker must implement.
type Processor[Env env.Env, In, Out msgBody] interface {
	Worker[Env]

	// Handle is the processor's specific method.
	// It processes the input message and returns the output message.
	Handle(ctx context.Context, msgIn *msg[In]) (*msg[Out], error)
}

// Egress interface defines the methods
// that an egress worker must implement.
type Egress[Env env.Env, In msgBody] interface {
	Worker[Env]

	// Deliver is the egress worker's specific method.
	// It takes the input message and it shall send the message
	// to the egress destination.
	Deliver(ctx context.Context, task *msg[In]) error
}

var _ Worker[env.Env] = (*BaseWorker[env.Env])(nil)

// BaseWorker is the base struct for a worker that can be embedded.
type BaseWorker[Env env.Env] struct {
	Env Env
	Tel *telemetry.Telemetry
}

// SetEnvironment sets the environment for the worker.
func (w *BaseWorker[Env]) SetEnvironment(env Env) {
	w.Env = env
	w.Tel = env.Telemetry()
}

// Init does nothing.
func (w *BaseWorker[Env]) Init(_ context.Context) error { return nil }

// Close does nothing.
func (w *BaseWorker[Env]) Close(_ context.Context) error { return nil }
