package processor

import (
	"context"
	"errors"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/processor/metrics"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Environment ────────────────────────────────────────────────────────────|

type teeEnv struct {
	*env.BaseEnv[*config.Empty, *metrics.TeeStage]
}

func newTeeEnv() *teeEnv {
	return &teeEnv{
		BaseEnv: env.NewProcessorEnv(config.NewEmpty(), metrics.NewTeeStage()),
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*teeEnv] = (*teeRunner[msgBody])(nil)

type teeRunner[T msgBody] struct {
	*teeEnv

	inConnector   msgConn[T]
	outConnectors []msgConn[T]

	runDone chan struct{}

	cloneCount int
}

func newTeeRunner[T msgBody](inConnector msgConn[T], outConnectors ...msgConn[T]) *teeRunner[T] {
	return &teeRunner[T]{
		inConnector:   inConnector,
		outConnectors: outConnectors,

		runDone: make(chan struct{}),
	}
}

func (tr *teeRunner[T]) SetEnvironment(env *teeEnv) {
	tr.teeEnv = env
}

func (tr *teeRunner[T]) Init(_ context.Context) error {
	tr.cloneCount = len(tr.outConnectors)
	if tr.cloneCount == 0 {
		return errors.New("no output connector specified")
	}

	return nil
}

func (tr *teeRunner[T]) Run(ctx context.Context) {
	defer close(tr.runDone)

	for {
		msgIn, err := tr.inConnector.Read(ctx)
		if err != nil {
			// This means the input connector is closed
			// and there are no more messages in it
			return
		}

		tr.GetProcessorMetrics().IncrementProcessedMessages()
		tr.clone(ctx, msgIn)
	}
}

func (tr *teeRunner[T]) clone(ctx context.Context, msgIn *msg[T]) {
	// Extract the span context from the input message
	ctx, span := tr.Telemetry().StartTrace(msgIn.LoadSpanContext(ctx), "clone message")
	defer span.End()

	span.SetAttributes(attribute.Int("clone_count", tr.cloneCount))

	for _, outConn := range tr.outConnectors {
		// Clone the input message
		msgOut := msgIn.Clone()

		if err := outConn.Write(msgOut); err != nil {
			// Destroy the cloned message, if the write fails
			msgOut.Destroy()
			tr.Telemetry().LogError("failed to write into output connector", err)
		}
	}

	tr.Metrics.IncrementClonedMessages()
}

func (tr *teeRunner[T]) Close(_ context.Context) {
	<-tr.runDone

	for _, outConn := range tr.outConnectors {
		outConn.Close()
	}
}

func (tr *teeRunner[T]) Inputs() []uintptr {
	return []uintptr{connector.GetConnectorID(tr.inConnector)}
}

func (tr *teeRunner[T]) Outputs() []uintptr {
	outputs := make([]uintptr, 0, len(tr.outConnectors))

	for _, outConn := range tr.outConnectors {
		outputs = append(outputs, connector.GetConnectorID(outConn))
	}

	return outputs
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*TeeStage[msgBody])(nil)

// TeeStage is a processor stage that "clones" the input message to multiple output connectors.
// Under the hood, it does not perform an actual copy of the real message data (envelope).
// It only copies the message's metadata and increments the reference counter of the enveloped message.
type TeeStage[T msgBody] struct {
	*stage.ProcessorStage[T, T, *teeEnv]
}

// NewTeeStage returns a new tee processor stage.
func NewTeeStage[T msgBody](inConnector msgConn[T], outConnectors ...msgConn[T]) *TeeStage[T] {
	return &TeeStage[T]{
		ProcessorStage: stage.NewProcessorStageFromRunner[T, T](
			"tee", newTeeEnv(), newTeeRunner(inConnector, outConnectors...),
		),
	}
}
