package egress

import (
	"context"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
)

// ─── Environment ────────────────────────────────────────────────────────────|

type sinkEnv struct {
	*env.BaseEnv[*config.Empty, *metrics.EmptyMetrics]
}

func newSinkEnv() *sinkEnv {
	return &sinkEnv{
		BaseEnv: env.NewEgressEnv(config.NewEmpty(), metrics.NewEmptyMetrics()),
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

type sinkRunner[T msgBody] struct {
	*sinkEnv

	inConnector msgConn[T]

	runDone chan struct{}
}

func newSinkRunner[T msgBody](inConnector msgConn[T]) *sinkRunner[T] {
	return &sinkRunner[T]{
		inConnector: inConnector,

		runDone: make(chan struct{}),
	}
}

func (sr *sinkRunner[T]) SetEnvironment(env *sinkEnv) {
	sr.sinkEnv = env
}

func (sr *sinkRunner[T]) Init(_ context.Context) error {
	return nil
}

func (sr *sinkRunner[T]) Run(ctx context.Context) {
	defer close(sr.runDone)

	for {
		msgIn, err := sr.inConnector.Read(ctx)
		if err != nil {
			// This means the input connector is closed
			// and there are no more messages in it
			return
		}

		metricsCtx := msgIn.LoadSpanContext(ctx)
		sr.GetEgressMetrics().IncrementDeliveredMessages()
		sr.GetEgressMetrics().RecordTotalMessageProcessingTime(
			metricsCtx, int(time.Since(msgIn.GetReceiveTime()).Milliseconds()),
		)

		msgIn.Destroy()
	}
}

func (sr *sinkRunner[T]) Close(_ context.Context) {
	<-sr.runDone
}

func (sr *sinkRunner[T]) Inputs() []uintptr {
	return []uintptr{connector.GetConnectorID(sr.inConnector)}
}

func (sr *sinkRunner[T]) Outputs() []uintptr {
	return []uintptr{}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// SinkStage is an egress stage that simply destroys all incoming messages.
// It is intended for testing purposes.
type SinkStage[T msgBody] struct {
	*stage.EgressStage[T, *sinkEnv]
}

// NewSinkStage returns a new sink egress stage.
func NewSinkStage[T msgBody](inConnector msgConn[T]) *SinkStage[T] {
	return &SinkStage[T]{
		EgressStage: stage.NewEgressStageFromRunner[T](
			"sink", newSinkEnv(), newSinkRunner(inConnector),
		),
	}
}
