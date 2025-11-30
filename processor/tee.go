package processor

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal"
	"go.opentelemetry.io/otel/attribute"
)

// TeeStage is a processor stage that "clones" the input message to multiple output connectors.
// Under the hood, it does not perform an actual copy of the real message data (envelope).
// It only copies the message's metadata and increments the reference counter of the enveloped message.
type TeeStage[T msgEnv] struct {
	tel *internal.Telemetry

	inputConnector   msgConn[T]
	outputConnectors []msgConn[T]

	cloneCount int

	// Metrics
	clonedMessages atomic.Int64
}

// NewTeeStage returns a new tee processor stage.
func NewTeeStage[T msgEnv](inputConnector msgConn[T], outputConnectors ...msgConn[T]) *TeeStage[T] {
	return &TeeStage[T]{
		tel: internal.NewTelemetry("processor", "tee"),

		inputConnector:   inputConnector,
		outputConnectors: outputConnectors,
	}
}

// Init initializes the stage.
func (ts *TeeStage[T]) Init(_ context.Context) error {
	ts.tel.LogInfo("initializing")

	cloneCount := len(ts.outputConnectors)
	if cloneCount == 0 {
		return errors.New("no output connector specified")
	}
	ts.cloneCount = cloneCount

	ts.initMetrics()

	return nil
}

func (ts *TeeStage[T]) initMetrics() {
	ts.tel.NewCounter("cloned_messages", func() int64 { return ts.clonedMessages.Load() })
}

// Run runs the stage.
func (ts *TeeStage[T]) Run(ctx context.Context) {
	ts.tel.LogInfo("running")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgIn, err := ts.inputConnector.Read()
		if err != nil {
			if errors.Is(err, connector.ErrClosed) {
				ts.tel.LogInfo("input connector is closed, stopping")
				return
			}

			if !errors.Is(err, connector.ErrReadTimeout) {
				ts.tel.LogError("failed to read from input connector", err)
			}

			continue
		}

		ts.clone(ctx, msgIn)
	}
}

func (ts *TeeStage[T]) clone(ctx context.Context, msgIn *msg[T]) {
	// Extract the span context from the input message
	ctx, span := ts.tel.NewTrace(msgIn.LoadSpanContext(ctx), "clone message")
	defer span.End()

	span.SetAttributes(attribute.Int("clone_count", ts.cloneCount))

	for _, outConn := range ts.outputConnectors {
		// Clone the input message
		msgOut := msgIn.Clone()

		if err := outConn.Write(msgOut); err != nil {
			// Destroy the cloned message, if the write fails
			msgOut.Destroy()
			ts.tel.LogError("failed to write into output connector", err)
		}
	}
}

// Close closes the stage.
func (ts *TeeStage[T]) Close() {
	ts.tel.LogInfo("closing")

	for _, outConn := range ts.outputConnectors {
		outConn.Close()
	}
}
