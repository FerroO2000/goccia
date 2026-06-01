package processor

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"github.com/FerroO2000/goccia/processor/metrics"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// FilterConfig structs contains the configuration for the [FilterStage].
type FilterConfig struct {
	*config.Base
}

// NewFilterConfig returns the default configuration for the [FilterStage].
func NewFilterConfig(runningMode config.StageRunningMode) *FilterConfig {
	return &FilterConfig{
		Base: config.NewBase(runningMode),
	}
}

// ─── Environment ────────────────────────────────────────────────────────────|

type filterEnv[T msgBody] struct {
	*env.BaseEnv[*FilterConfig, *metrics.FilterStage]

	filterFn func(T) bool
}

func newFilterEnv[T msgBody](config *FilterConfig, filterFn func(T) bool) *filterEnv[T] {
	return &filterEnv[T]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewFilterStage()),

		filterFn: filterFn,
	}
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type filterWorker[T msgBody] struct {
	worker.BaseWorker[*filterEnv[T]]
}

func newFilterWorkerMaker[T msgBody]() func() *filterWorker[T] {
	return func() *filterWorker[T] {
		return &filterWorker[T]{}
	}
}

func (fw *filterWorker[T]) Handle(ctx context.Context, msgIn *msg[T]) (*msg[T], error) {
	// Extract the span context from the input message
	_, span := fw.Tel.StartTrace(msgIn.LoadSpanContext(ctx), "filter message")
	defer span.End()

	if !fw.Env.filterFn(msgIn.GetBody()) {
		msgIn.Drop()

		fw.Env.Metrics.IncrementFilteredMessages()
	}

	return msgIn, nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*FilterStage[msgBody])(nil)

// FilterStage is a processor stage that filters messages based on a user-defined function.
type FilterStage[T msgBody] struct {
	*stage.ProcessorStage[T, T, *filterEnv[T]]
}

// NewFilterStage returns a new filter processor stage.
func NewFilterStage[T msgBody](filterFn func(T) bool, inputConnector, outConnector msgConn[T], cfg *FilterConfig) *FilterStage[T] {
	env := newFilterEnv(cfg, filterFn)

	return &FilterStage[T]{
		ProcessorStage: stage.NewProcessorStage(
			"filter", inputConnector, outConnector, env, newFilterWorkerMaker[T](), cfg.Stage,
		),
	}
}
