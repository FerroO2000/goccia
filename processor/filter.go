package processor

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/pool"
)

//////////////
//  CONFIG  //
//////////////

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

////////////////////////
//  WORKER ARGUMENTS  //
////////////////////////

type filterWorkerArgs[T msgBody] struct {
	filterFn func(T) bool
}

func newFilterWorkerArgs[T msgBody](filterFn func(T) bool) *filterWorkerArgs[T] {
	return &filterWorkerArgs[T]{
		filterFn: filterFn,
	}
}

//////////////////////
//  WORKER METRICS  //
//////////////////////

type filterWorkerMetrics struct {
	once sync.Once

	filteredMessages atomic.Int64
}

var filterWorkerMetricsInst = &filterWorkerMetrics{}

func (fwm *filterWorkerMetrics) init(tel *internal.Telemetry) {
	fwm.once.Do(func() {
		fwm.initMetrics(tel)
	})
}

func (fwm *filterWorkerMetrics) initMetrics(tel *internal.Telemetry) {
	tel.NewCounter("filtered_messages", func() int64 { return fwm.filteredMessages.Load() })
}

func (fwm *filterWorkerMetrics) incrementFilteredMessages() {
	fwm.filteredMessages.Add(1)
}

/////////////////////////////
//  WORKER IMPLEMENTATION  //
/////////////////////////////

type filterWorker[T msgBody] struct {
	pool.BaseWorker

	filterFn func(T) bool

	metrics *filterWorkerMetrics
}

func newFilterWorkerInstMaker[T msgBody]() workerInstanceMaker[*filterWorkerArgs[T], T, T] {
	return func() workerInstance[*filterWorkerArgs[T], T, T] {
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
	_, span := fw.Tel.NewTrace(msgIn.LoadSpanContext(ctx), "filter message")
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

/////////////
//  STAGE  //
/////////////

// FilterStage is a processor stage that filters messages based on a user-defined function.
type FilterStage[T msgBody] struct {
	stage[*filterWorkerArgs[T], T, T, *FilterConfig]

	filterFn func(T) bool
}

// NewFilterStage returns a new filter processor stage.
func NewFilterStage[T msgBody](filterFn func(T) bool, inputConnector, outputConnector msgConn[T], cfg *FilterConfig) *FilterStage[T] {
	return &FilterStage[T]{
		stage: newStage(
			"filter", inputConnector, outputConnector, newFilterWorkerInstMaker[T](), cfg,
		),

		filterFn: filterFn,
	}
}

// Init initializes the stage.
func (fs *FilterStage[T]) Init(ctx context.Context) error {
	return fs.stage.Init(ctx, newFilterWorkerArgs(fs.filterFn))
}
