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

type teeEnv[T msgBody] struct {
	*env.BaseEnv[*config.Empty, *metrics.TeeStage]

	inConnector   msgConn[T]
	outConnectors []msgConn[T]

	cloneCount int
}

func newTeeEnv[T msgBody](inConnector msgConn[T], outConnectors ...msgConn[T]) *teeEnv[T] {
	return &teeEnv[T]{
		BaseEnv: env.NewProcessorEnv(config.NewEmpty(), metrics.NewTeeStage()),

		inConnector:   inConnector,
		outConnectors: outConnectors,
	}
}

func (te *teeEnv[T]) Init(ctx context.Context) error {
	te.cloneCount = len(te.outConnectors)
	if te.cloneCount == 0 {
		return errors.New("no output connector specified")
	}

	return te.BaseEnv.Init(ctx)
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*teeEnv[msgBody]] = (*teeRunner[msgBody])(nil)

type teeRunner[T msgBody] struct {
	env *teeEnv[T]

	runDone chan struct{}
}

func newTeeRunner[T msgBody]() *teeRunner[T] {
	return &teeRunner[T]{
		runDone: make(chan struct{}),
	}
}

func (tr *teeRunner[T]) SetEnvironment(env *teeEnv[T]) {
	tr.env = env
}

func (tr *teeRunner[T]) Init(_ context.Context) error {
	return nil
}

func (tr *teeRunner[T]) Run(ctx context.Context) {
	defer close(tr.runDone)

	for {
		msgIn, err := tr.env.inConnector.Read(ctx)
		if err != nil {
			// This means the input connector is closed
			// and there are no more messages in it
			return
		}

		tr.clone(ctx, msgIn)
	}
}

func (tr *teeRunner[T]) clone(ctx context.Context, msgIn *msg[T]) {
	// Extract the span context from the input message
	ctx, span := tr.env.Telemetry().StartTrace(msgIn.LoadSpanContext(ctx), "clone message")
	defer span.End()

	span.SetAttributes(attribute.Int("clone_count", tr.env.cloneCount))

	for _, outConn := range tr.env.outConnectors {
		// Clone the input message
		msgOut := msgIn.Clone()

		if err := outConn.Write(msgOut); err != nil {
			// Destroy the cloned message, if the write fails
			msgOut.Destroy()
			tr.env.Telemetry().LogError("failed to write into output connector", err)
		}
	}

	tr.env.Metrics.IncrementClonedMessages()
}

func (tr *teeRunner[T]) Close(_ context.Context) {
	<-tr.runDone

	for _, outConn := range tr.env.outConnectors {
		outConn.Close()
	}
}

func (tr *teeRunner[T]) Inputs() []uintptr {
	return []uintptr{connector.GetConnectorID(tr.env.inConnector)}
}

func (tr *teeRunner[T]) Outputs() []uintptr {
	outputs := make([]uintptr, 0, len(tr.env.outConnectors))

	for _, outConn := range tr.env.outConnectors {
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
	*stage.ProcessorStage[T, T, *teeEnv[T]]
}

// NewTeeStage returns a new tee processor stage.
func NewTeeStage[T msgBody](inConnector msgConn[T], outConnectors ...msgConn[T]) *TeeStage[T] {

	env := newTeeEnv(inConnector, outConnectors...)

	return &TeeStage[T]{
		ProcessorStage: stage.NewProcessorStageFromRunner[T, T](
			"tee", env, newTeeRunner[T](),
		),
	}
}
