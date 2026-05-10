package processor

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/pool"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/telemetry"
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

// ─── Worker - Arguments ─────────────────────────────────────────────────────|

type filterWorkerArgs[T msgBody] struct {
	filterFn func(T) bool
}

func newFilterWorkerArgs[T msgBody](filterFn func(T) bool) *filterWorkerArgs[T] {
	return &filterWorkerArgs[T]{
		filterFn: filterFn,
	}
}

// ─── Worker - Metrics ───────────────────────────────────────────────────────|

type filterWorkerMetrics struct {
	once sync.Once

	filteredMessages atomic.Int64
}

var filterWorkerMetricsInst = &filterWorkerMetrics{}

func (fwm *filterWorkerMetrics) init(tel *telemetry.Telemetry) {
	fwm.once.Do(func() {
		fwm.initMetrics(tel)
	})
}

func (fwm *filterWorkerMetrics) initMetrics(tel *telemetry.Telemetry) {
	tel.NewCounterMetric("filtered_messages", func() int64 { return fwm.filteredMessages.Load() })
}

func (fwm *filterWorkerMetrics) incrementFilteredMessages() {
	fwm.filteredMessages.Add(1)
}

// ─── Worker - Implementation ────────────────────────────────────────────────|

type filterWorker[T msgBody] struct {
	pool.BaseWorker

	filterFn func(T) bool

	metrics *filterWorkerMetrics
}

func newFilterWorkerMaker[T msgBody]() func() *filterWorker[T] {
	return func() *filterWorker[T] {
		return &filterWorker[T]{
			metrics: filterWorkerMetricsInst,
		}
	}
}

func (fw *filterWorker[T]) Init(_ context.Context, args *filterWorkerArgs[T]) error {
	fw.filterFn = args.filterFn

	fw.metrics.init(fw.Tel)

	return nil
}

func (fw *filterWorker[T]) Handle(ctx context.Context, msgIn *msg[T]) (*msg[T], error) {
	// Extract the span context from the input message
	_, span := fw.Tel.StartTrace(msgIn.LoadSpanContext(ctx), "filter message")
	defer span.End()

	if !fw.filterFn(msgIn.GetBody()) {
		msgIn.Drop()

		fw.metrics.incrementFilteredMessages()
	}

	return msgIn, nil
}

func (fw *filterWorker[T]) Close(_ context.Context) error {
	return nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*FilterStage[msgBody])(nil)

// FilterStage is a processor stage that filters messages based on a user-defined function.
type FilterStage[T msgBody] struct {
	*stage.ProcessorStage[T, T, *filterWorkerArgs[T], *FilterConfig]

	filterFn func(T) bool
}

// NewFilterStage returns a new filter processor stage.
func NewFilterStage[T msgBody](filterFn func(T) bool, inputConnector, outputConnector msgConn[T], cfg *FilterConfig) *FilterStage[T] {
	return &FilterStage[T]{
		ProcessorStage: stage.NewProcessorStage(
			"filter", inputConnector, outputConnector, newFilterWorkerMaker[T](), cfg,
		),

		filterFn: filterFn,
	}
}

// Init initializes the stage.
func (fs *FilterStage[T]) Init(ctx context.Context) error {
	return fs.ProcessorStage.InitWithArgs(ctx, newFilterWorkerArgs(fs.filterFn))
}
