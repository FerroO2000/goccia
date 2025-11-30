package egress

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/pool"
	"go.opentelemetry.io/otel/metric"
)

////////////////
//  INSTANCE  //
////////////////

type workerInstance[Args any, In msgEnv] interface {
	Init(ctx context.Context, args Args) error
	Close(ctx context.Context) error
	SetTelemetry(tel *internal.Telemetry)
	Deliver(ctx context.Context, task *msg[In]) error
}

type workerInstanceMaker[Args any, In msgEnv] func() workerInstance[Args, In]

///////////////
//  METRICS  //
///////////////

type workerMetrics struct {
	tel *internal.Telemetry

	deliveredMessages atomic.Int64
	deliveringErrors  atomic.Int64

	totMsgProcessingTime *internal.Histogram
}

func newWorkerMetrics(tel *internal.Telemetry) *workerMetrics {
	return &workerMetrics{
		tel: tel,
	}
}

func (wm *workerMetrics) init() {
	wm.tel.NewCounter("delivered_messages", func() int64 { return wm.deliveredMessages.Load() })
	wm.tel.NewCounter("delivering_errors", func() int64 { return wm.deliveringErrors.Load() })

	wm.totMsgProcessingTime = wm.tel.NewHistogram("total_message_processing_time", metric.WithUnit("ms"))
}

func (wm *workerMetrics) incrementDeliveredMessages() {
	wm.deliveredMessages.Add(1)
}

func (wm *workerMetrics) incrementDeliveringErrors() {
	wm.deliveringErrors.Add(1)
}

func (wm *workerMetrics) recordTotalMessageProcessingTime(ctx context.Context, recvTime time.Time) {
	wm.totMsgProcessingTime.Record(ctx, time.Since(recvTime).Milliseconds())
}

//////////////
//  WORKER  //
//////////////

type worker[Args any, In msgEnv] struct {
	tel *internal.Telemetry

	id   int
	inst workerInstance[Args, In]

	metrics *workerMetrics
}

func newWorker[Args any, In msgEnv](
	tel *internal.Telemetry, id int, inst workerInstance[Args, In], metrics *workerMetrics,
) *worker[Args, In] {

	return &worker[Args, In]{
		tel: tel,

		id:   id,
		inst: inst,

		metrics: metrics,
	}
}

func (w *worker[Args, In]) init(ctx context.Context, args Args) error {
	w.tel.LogInfo("initializing worker", "worker_id", w.id)

	w.inst.SetTelemetry(w.tel)

	if err := w.inst.Init(ctx, args); err != nil {
		w.tel.LogError("failed to init worker", err, "worker_id", w.id)
		return err
	}

	return w.inst.Init(ctx, args)
}

func (w *worker[Args, In]) deliver(ctx context.Context, msgIn *msg[In]) {
	defer msgIn.Destroy()

	// Extract the span context from the input message
	ctx = msgIn.LoadSpanContext(ctx)

	if err := w.inst.Deliver(ctx, msgIn); err != nil {
		w.tel.LogError("failed to deliver message", err, "worker_id", w.id)
		w.metrics.incrementDeliveringErrors()
	}

	w.metrics.incrementDeliveredMessages()
	w.metrics.recordTotalMessageProcessingTime(ctx, msgIn.GetReceiveTime())
}

func (w *worker[Args, In]) close(ctx context.Context) {
	w.tel.LogInfo("closing worker", "worker_id", w.id)

	if err := w.inst.Close(ctx); err != nil {
		w.tel.LogError("failed to close worker", err, "worker_id", w.id)
	}
}

////////////
//  POOL  //
////////////

type workerPool[Args any, In msgEnv] struct {
	tel *internal.Telemetry

	cfg *pool.Config

	scaler *pool.Scaler

	workerArgs      Args
	workerInstMaker workerInstanceMaker[Args, In]

	wg *sync.WaitGroup

	fanOut *pool.FanOut[*msg[In]]

	metrics *workerMetrics
}

func newWorkerPool[Args any, In msgEnv](
	tel *internal.Telemetry, workerInstMaker workerInstanceMaker[Args, In], cfg *pool.Config,
) *workerPool[Args, In] {

	return &workerPool[Args, In]{
		tel: tel,

		cfg: cfg,

		scaler: pool.NewScaler(tel, cfg),

		workerInstMaker: workerInstMaker,

		wg: &sync.WaitGroup{},

		fanOut: pool.NewFanOut[*msg[In]](cfg.InputQueueSize),

		metrics: newWorkerMetrics(tel),
	}
}

func (wp *workerPool[Args, In]) init(ctx context.Context, workerArgs Args) error {
	wp.workerArgs = workerArgs

	wp.scaler.Init(ctx, wp.cfg.InitialWorkers)
	wp.metrics.init()

	return nil
}

func (wp *workerPool[Args, In]) run(ctx context.Context) {
	go wp.runStartWorkerListener(ctx)
	go wp.scaler.Run(ctx)
}

func (wp *workerPool[Args, In]) runStartWorkerListener(ctx context.Context) {
	startCh := wp.scaler.GetStartCh()

	for {
		select {
		case <-ctx.Done():
			return

		case <-startCh:
			go wp.runWorker(ctx)
		}
	}
}

func (wp *workerPool[Args, In]) runWorker(ctx context.Context) {
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

			worker.deliver(ctx, msgIn)

			wp.scaler.NotifyTaskCompleted()
		}
	}
}

func (wp *workerPool[Args, In]) close() {
	wp.tel.LogInfo("closing worker pool")

	wp.fanOut.Close()

	wp.wg.Wait()
	wp.scaler.Close()
}

func (wp *workerPool[Args, In]) addMessage(ctx context.Context, msgIn *msg[In]) error {
	if err := wp.fanOut.AddTask(ctx, msgIn); err != nil {
		return err
	}

	wp.scaler.NotifyTaskAdded()

	return nil
}
