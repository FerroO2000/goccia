package processor

import (
	"context"
	"errors"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/FerroO2000/goccia/processor/metrics"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Arguments ──────────────────────────────────────────────────────────────|

type teeArgs[T msgBody] struct {
	inConnector   msgConn[T]
	outConnectors []msgConn[T]
}

func newTeeArgs[T msgBody](inConnector msgConn[T], outConnectors ...msgConn[T]) *teeArgs[T] {
	return &teeArgs[T]{
		inConnector:   inConnector,
		outConnectors: outConnectors,
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*teeArgs[msgBody]] = (*teeRunner[msgBody])(nil)

type teeRunner[T msgBody] struct {
	tel *telemetry.Telemetry

	inConnector   msgConn[T]
	outConnectors []msgConn[T]

	cloneCount int

	runDone chan struct{}

	metrics *metrics.TeeStage
}

func newTeeRunner[T msgBody]() *teeRunner[T] {
	return &teeRunner[T]{
		runDone: make(chan struct{}),

		metrics: metrics.NewTeeStage(),
	}
}

func (tr *teeRunner[T]) SetTelemetry(tel *telemetry.Telemetry) {
	tr.tel = tel
}

func (tr *teeRunner[T]) Init(_ context.Context, args *teeArgs[T]) error {
	tr.cloneCount = len(args.outConnectors)
	if tr.cloneCount == 0 {
		return errors.New("no output connector specified")
	}

	tr.inConnector = args.inConnector
	tr.outConnectors = args.outConnectors

	return tr.metrics.InitMetrics(tr.tel)
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

		tr.clone(ctx, msgIn)
	}
}

func (tr *teeRunner[T]) clone(ctx context.Context, msgIn *msg[T]) {
	// Extract the span context from the input message
	ctx, span := tr.tel.StartTrace(msgIn.LoadSpanContext(ctx), "clone message")
	defer span.End()

	span.SetAttributes(attribute.Int("clone_count", tr.cloneCount))

	for _, outConn := range tr.outConnectors {
		// Clone the input message
		msgOut := msgIn.Clone()

		if err := outConn.Write(msgOut); err != nil {
			// Destroy the cloned message, if the write fails
			msgOut.Destroy()
			tr.tel.LogError("failed to write into output connector", err)
		}
	}

	tr.metrics.IncrementClonedMessages()
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
	*stage.ProcessorStage[T, T, *teeArgs[T], *config.Dummy]

	args *teeArgs[T]
}

// NewTeeStage returns a new tee processor stage.
func NewTeeStage[T msgBody](inConnector msgConn[T], outConnectors ...msgConn[T]) *TeeStage[T] {
	return &TeeStage[T]{
		ProcessorStage: stage.NewProcessorStageFromRunner[T, T](
			"tee", newTeeRunner[T](), &config.Dummy{},
		),

		args: newTeeArgs(inConnector, outConnectors...),
	}
}

// Init initializes the stage.
func (ts *TeeStage[T]) Init(ctx context.Context) error {
	return ts.ProcessorStage.InitWithArgs(ctx, ts.args)
}
