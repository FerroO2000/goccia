package stage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/pool"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

type msg[T msgBody] = message.Message[T]

type msgBody = message.Body

type msgConn[T msgBody] = connector.MessageConnector[T]

// ─── Worker Metrics ─────────────────────────────────────────────────────────|

type processorWorkerMetrics struct {
	tel *telemetry.Telemetry

	processedMessages atomic.Int64
	droppedMessages   atomic.Int64
	processingErrors  atomic.Int64
}

func newProcessorWorkerMetrics(tel *telemetry.Telemetry) *processorWorkerMetrics {
	return &processorWorkerMetrics{
		tel: tel,
	}
}

func (wm *processorWorkerMetrics) init() {
	wm.tel.NewCounterMetric("processed_messages", func() int64 { return wm.processedMessages.Load() })
	wm.tel.NewCounterMetric("dropped_messages", func() int64 { return wm.droppedMessages.Load() })
	wm.tel.NewCounterMetric("processing_errors", func() int64 { return wm.processingErrors.Load() })
}

func (wm *processorWorkerMetrics) incrementProcessedMessages() {
	wm.processedMessages.Add(1)
}

func (wm *processorWorkerMetrics) incrementDroppedMessages() {
	wm.droppedMessages.Add(1)
}

func (wm *processorWorkerMetrics) incrementProcessingErrors() {
	wm.processingErrors.Add(1)
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type ProcessorWorkerInstance[Args any, In, Out msgBody] interface {
	Init(ctx context.Context, args Args) error
	Handle(ctx context.Context, msgIn *msg[In]) (msgOut *msg[Out], err error)
	Close(ctx context.Context) error
	SetTelemetry(tel *telemetry.Telemetry)
}

type processorWorkerInstanceMaker[Args any, In, Out msgBody] func() ProcessorWorkerInstance[Args, In, Out]

type ProcessorWorker[Args any, In, Out msgBody] struct {
	tel *telemetry.Telemetry

	id       int
	instance ProcessorWorkerInstance[Args, In, Out]

	metrics *processorWorkerMetrics
}

func newProcessorWorker[Args any, In, Out msgBody](
	tel *telemetry.Telemetry, id int, instance ProcessorWorkerInstance[Args, In, Out], metrics *processorWorkerMetrics,
) *ProcessorWorker[Args, In, Out] {

	return &ProcessorWorker[Args, In, Out]{
		tel: tel,

		id:       id,
		instance: instance,

		metrics: metrics,
	}
}

func (w *ProcessorWorker[Args, In, Out]) init(ctx context.Context, args Args) error {
	w.tel.LogDebug("initializing processor worker", "worker_id", w.id)
	defer w.tel.LogDebug("initialized processor worker", "worker_id", w.id)

	w.metrics.init()

	w.instance.SetTelemetry(w.tel)

	if err := w.instance.Init(ctx, args); err != nil {
		w.tel.LogError("failed to init processor worker", err, "worker_id", w.id)
		return err
	}

	return nil
}

func (w *ProcessorWorker[Args, In, Out]) process(ctx context.Context, msgIn *msg[In]) (*msg[Out], bool) {
	defer msgIn.Destroy()

	w.metrics.incrementProcessedMessages()

	// Extract the span context from the input message
	ctx = msgIn.LoadSpanContext(ctx)

	msgOut, err := w.instance.Handle(ctx, msgIn)
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

func (w *ProcessorWorker[Args, In, Out]) close(ctx context.Context) {
	w.tel.LogDebug("closing processor worker", "worker_id", w.id)
	defer w.tel.LogDebug("closed processor worker", "worker_id", w.id)

	if err := w.instance.Close(ctx); err != nil {
		w.tel.LogError("failed to close processor worker", err, "worker_id", w.id)
	}
}

// ─── Worker Pool ────────────────────────────────────────────────────────────|

// type workerPool[WArgs any] struct {
// 	tel *telemetry.Telemetry

// 	poolCfg *config.Pool

// 	scaler *pool.Scaler

// 	workerArgs      WArgs
// 	workerInstMaker func() ProcessorWorkerInstance[WArgs, msgBody, msgBody]

// 	wg *sync.WaitGroup

// 	fanOut *rb.RingBuffer[*msg[msgBody]]

// 	metrics *processorWorkerMetrics
// }

// func (wp *workerPool[WArgs]) runStarter(ctx context.Context) {

// 	startWorkerCh := wp.scaler.GetStartCh()

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return

// 		case <-startWorkerCh:
// 			wp.wg.Add(1)
// 			go wp.runWorker(ctx)
// 		}
// 	}
// }

// func (wp *workerPool[WArgs]) spawnWorker() (*ProcessorWorker[WArgs, msgBody, msgBody], <-chan struct{}) {
// 	workerID := wp.scaler.NotifyWorkerStart()
// 	workerInst := wp.workerInstMaker()
// 	worker := newProcessorWorker(wp.tel, workerID, workerInst, wp.metrics)

// 	stopCh := wp.scaler.GetStopCh(workerID)
// 	if stopCh == nil {
// 		return nil, nil
// 	}

// 	return worker, stopCh
// }

// func (wp *workerPool[WArgs]) runWorker(ctx context.Context) {
// 	defer wp.wg.Done()

// 	worker, stopCh := wp.spawnWorker()
// 	if worker == nil || stopCh == nil {
// 		return
// 	}

// 	defer wp.scaler.NotifyWorkerStop()
// 	defer worker.close(ctx)

// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return

// 		case <-stopCh:
// 			return

// 		default:
// 			msgIn, err := wp.fanOut.Read(ctx)
// 			if err != nil {
// 				continue
// 			}

// 			if msgOut, valid := worker.process(ctx, msgIn); valid {
// 				if err := wp.fanIn.AddTask(msgOut); err != nil {
// 					wp.tel.LogError("failed to fan-in task", err)
// 				}
// 			}

// 			wp.scaler.NotifyTaskCompleted()
// 		}
// 	}
// }

type ProcessorWorkerPool[WArgs any, In, Out msgBody] struct {
	tel *telemetry.Telemetry

	poolCfg *config.Pool

	scaler *pool.Scaler

	workerArgs      WArgs
	workerInstMaker processorWorkerInstanceMaker[WArgs, In, Out]

	wg             *sync.WaitGroup
	listenerDoneCh chan struct{}

	fanOut *pool.FanOut[*msg[In]]
	fanIn  *pool.FanIn[*msg[Out]]

	metrics *processorWorkerMetrics
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

type BaseProcessorStage[WArgs any, In, Out msgBody, Cfg config.Config] struct {
	*BaseStage[Cfg]

	inputConnector  msgConn[In]
	outputConnector msgConn[Out]

	worker *ProcessorWorker[WArgs, In, Out]
}

func NewBaseProcessorStage[WArgs any, In, Out msgBody, Cfg config.Config](
	name string, inputConnector msgConn[In], outputConnector msgConn[Out], cfg Cfg,
) *BaseProcessorStage[WArgs, In, Out, Cfg] {

	return &BaseProcessorStage[WArgs, In, Out, Cfg]{
		BaseStage: newBaseStage(KindProcessor, name, cfg),

		inputConnector:  inputConnector,
		outputConnector: outputConnector,
	}
}

func (s *BaseProcessorStage[WArgs, In, Out, Cfg]) Inputs() []uintptr {
	return []uintptr{connector.GetConnectorID(s.inputConnector)}
}

func (s *BaseProcessorStage[WArgs, In, Out, Cfg]) Outputs() []uintptr {
	return []uintptr{connector.GetConnectorID(s.outputConnector)}
}

func (s *BaseProcessorStage[WArgs, In, Out, Cfg]) Init(ctx context.Context, workerArgs WArgs) error {
	s.tel.LogDebug("initializing processor stage")
	defer s.tel.LogDebug("initialized processor stage")

	if err := s.BaseStage.Init(ctx); err != nil {
		return err
	}

	return s.worker.init(ctx, workerArgs)
}

func (s *BaseProcessorStage[WArgs, In, Out, Cfg]) processorLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgIn, err := s.inputConnector.Read(ctx)
		if err != nil {
			// If got an error, it shall be one of the following:
			// - the input connector is closed
			// - the context is canceled
			// Both mean the loop shall be stopped,
			// because it means the connector is empty
			if errors.Is(err, connector.ErrClosed) || errors.Is(err, context.Canceled) {
				return
			}

			s.tel.LogError("failed to read from input connector", err)
			continue
		}

		// Process the input message in the worker
		msgOut, valid := s.worker.process(ctx, msgIn)

		// Check if the message is dropped
		if !valid {
			msgOut.Destroy()
			continue
		}

		// Write the message to the output connector
		if err := s.outputConnector.Write(msgOut); err != nil {
			msgOut.Destroy()
			s.Telemetry().LogError("failed to write into output connector", err)
		}
	}
}

func (s *BaseProcessorStage[WArgs, In, Out, Cfg]) Run(ctx context.Context) {
	s.tel.LogDebug("processor stage is running")
	defer s.tel.LogDebug("processor stage is stopped")
	s.processorLoop(ctx)
}

func (s *BaseProcessorStage[WArgs, In, Out, Cfg]) Drain(ctx context.Context) {
	s.tel.LogDebug("draining processor stage")
	defer s.tel.LogDebug("drained processor stage")
	s.processorLoop(ctx)
}

func (s *BaseProcessorStage[WArgs, In, Out, Cfg]) Close() {
	s.tel.LogDebug("closing processor stage")
	defer s.tel.LogDebug("processor stage closed")

	s.worker.close(context.Background())
	s.outputConnector.Close()
}
