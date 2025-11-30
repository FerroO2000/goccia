package processor

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/pool"
)

////////////////
//  INSTANCE  //
////////////////

type workerInstance[Args any, In, Out msgEnv] interface {
	Init(ctx context.Context, args Args) error
	Close(ctx context.Context) error
	SetTelemetry(tel *internal.Telemetry)
	Handle(ctx context.Context, task *msg[In]) (*msg[Out], error)
}

type workerInstanceMaker[Args any, In, Out msgEnv] func() workerInstance[Args, In, Out]

///////////////
//  METRICS  //
///////////////

type workerMetrics struct {
	tel *internal.Telemetry

	processedMessages atomic.Int64
	droppedMessages   atomic.Int64
	processingErrors  atomic.Int64
}

func newWorkerMetrics(tel *internal.Telemetry) *workerMetrics {
	return &workerMetrics{
		tel: tel,
	}
}

func (wm *workerMetrics) init() {
	wm.tel.NewCounter("processed_messages", func() int64 { return wm.processedMessages.Load() })
	wm.tel.NewCounter("dropped_messages", func() int64 { return wm.droppedMessages.Load() })
	wm.tel.NewCounter("processing_errors", func() int64 { return wm.processingErrors.Load() })
}

func (wm *workerMetrics) incrementProcessedMessages() {
	wm.processedMessages.Add(1)
}

func (wm *workerMetrics) incrementDroppedMessages() {
	wm.droppedMessages.Add(1)
}

func (wm *workerMetrics) incrementProcessingErrors() {
	wm.processingErrors.Add(1)
}

//////////////
//  WORKER  //
//////////////

type worker[Args any, In, Out msgEnv] struct {
	tel *internal.Telemetry

	id   int
	inst workerInstance[Args, In, Out]

	metrics *workerMetrics
}

func newWorker[Args any, In, Out msgEnv](
	tel *internal.Telemetry, id int, inst workerInstance[Args, In, Out], metrics *workerMetrics,
) *worker[Args, In, Out] {
	return &worker[Args, In, Out]{
		tel: tel,

		id:   id,
		inst: inst,

		metrics: metrics,
	}
}

func (w *worker[Args, In, Out]) init(ctx context.Context, args Args) error {
	w.tel.LogInfo("initializing worker", "worker_id", w.id)

	w.inst.SetTelemetry(w.tel)

	if err := w.inst.Init(ctx, args); err != nil {
		w.tel.LogError("failed to init worker", err, "worker_id", w.id)
		return err
	}

	return w.inst.Init(ctx, args)
}

func (w *worker[Args, In, Out]) process(ctx context.Context, msgIn *msg[In]) (*msg[Out], bool) {
	defer msgIn.Destroy()

	w.metrics.incrementProcessedMessages()

	// Extract the span context from the input message
	ctx = msgIn.LoadSpanContext(ctx)

	msgOut, err := w.inst.Handle(ctx, msgIn)
	if err != nil {
		w.tel.LogError("failed to process message", err, "worker_id", w.id)
		w.metrics.incrementProcessingErrors()

		return msgOut, false
	}

	// Set the receive time and timestamp
	msgOut.SetReceiveTime(msgIn.GetReceiveTime())
	msgOut.SetTimestamp(msgIn.GetTimestamp())

	// Check if the output message is valid for further processing
	valid := true
	if msgOut.IsDropped() {
		w.metrics.incrementDroppedMessages()
		valid = false
	}

	return msgOut, valid
}

func (w *worker[Args, In, Out]) close(ctx context.Context) {
	w.tel.LogInfo("closing worker", "worker_id", w.id)

	if err := w.inst.Close(ctx); err != nil {
		w.tel.LogError("failed to close worker", err, "worker_id", w.id)
	}
}

////////////
//  POOL  //
////////////

type workerPool[WArgs any, In, Out msgEnv] struct {
	tel *internal.Telemetry

	cfg *pool.Config

	scaler *pool.Scaler

	workerArgs      WArgs
	workerInstMaker workerInstanceMaker[WArgs, In, Out]

	wg *sync.WaitGroup

	fanOut *pool.FanOut[*msg[In]]
	fanIn  *pool.FanIn[*msg[Out]]

	metrics *workerMetrics
}

func newWorkerPool[WArgs any, In, Out msgEnv](
	tel *internal.Telemetry, workerInstMaker workerInstanceMaker[WArgs, In, Out], cfg *pool.Config,
) *workerPool[WArgs, In, Out] {

	return &workerPool[WArgs, In, Out]{
		tel: tel,

		cfg: cfg,

		scaler: pool.NewScaler(tel, cfg),

		workerInstMaker: workerInstMaker,

		wg: &sync.WaitGroup{},

		fanOut: pool.NewFanOut[*msg[In]](cfg.InputQueueSize),
		fanIn:  pool.NewFanIn[*msg[Out]](cfg.OutputQueueSize),

		metrics: newWorkerMetrics(tel),
	}
}

func (wp *workerPool[WArgs, In, Out]) init(ctx context.Context, workerArgs WArgs) error {
	wp.workerArgs = workerArgs

	wp.scaler.Init(ctx, wp.cfg.InitialWorkers)
	wp.metrics.init()

	return nil
}

func (wp *workerPool[WArgs, In, Out]) run(ctx context.Context) {
	wp.tel.LogInfo("running worker pool")

	go wp.runStartWorkerListener(ctx)
	go wp.scaler.Run(ctx)
}

func (wp *workerPool[WArgs, In, Out]) runStartWorkerListener(ctx context.Context) {
	startWorkerCh := wp.scaler.GetStartCh()

	for {
		select {
		case <-ctx.Done():
			return

		case <-startWorkerCh:
			go wp.runWorker(ctx)
		}
	}
}

func (wp *workerPool[WArgs, In, Out]) runWorker(ctx context.Context) {
	wp.wg.Add(1)
	defer wp.wg.Done()

	workerID := wp.scaler.NotifyWorkerStart()
	defer wp.scaler.NotifyWorkerStop()

	workerInst := wp.workerInstMaker()
	worker := newWorker(wp.tel, workerID, workerInst, wp.metrics)

	if err := worker.init(ctx, wp.workerArgs); err != nil {
		return
	}

	defer worker.close(ctx)

	stopCh := wp.scaler.GetStopCh(workerID)
	if stopCh == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-stopCh:
			return

		default:
			msgIn, err := wp.fanOut.ReadTask()
			if err != nil {
				continue
			}

			if msgOut, valid := worker.process(ctx, msgIn); valid {
				if err := wp.fanIn.AddTask(msgOut); err != nil {
					wp.tel.LogError("failed to fan-in task", err)
				}
			}

			wp.scaler.NotifyTaskCompleted()
		}
	}
}

func (wp *workerPool[WArgs, In, Out]) close() {
	wp.tel.LogInfo("closing worker pool")

	wp.fanOut.Close()

	wp.wg.Wait()
	wp.scaler.Close()

	wp.fanIn.Close()
}

func (wp *workerPool[WArgs, In, Out]) addMessage(ctx context.Context, msgIn *msg[In]) error {
	if err := wp.fanOut.AddTask(ctx, msgIn); err != nil {
		return err
	}

	wp.scaler.NotifyTaskAdded()

	return nil
}

func (wp *workerPool[WArgs, In, Out]) extractMessage() (*msg[Out], error) {
	return wp.fanIn.ReadTask()
}
