package worker

import (
	"context"
	"sync/atomic"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// Runner is a struct that represents a worker runner.
// It is used as a wrapper for the different kinds of worker,
// and it is meant to be used by both single-threaded and pooled stages.
type Runner[WArgs any, W Worker[WArgs]] struct {
	tel *telemetry.Telemetry

	workerHandler workerHandler[WArgs, W]
}

func newRunner[WArgs any, W Worker[WArgs]](
	tel *telemetry.Telemetry, workerHandler workerHandler[WArgs, W],
) *Runner[WArgs, W] {

	return &Runner[WArgs, W]{
		tel: tel,

		workerHandler: workerHandler,
	}
}

// NewProcessorRunner returns a new runner for a processor worker.
func NewProcessorRunner[WArgs any, In, Out msgBody, W Processor[WArgs, In, Out]](
	tel *telemetry.Telemetry, stageMetrics *metrics.ProcessorStage,
	workerID int, worker W,
	messageReader connector.MessageConnector[In], messageWriter connector.MessageConnector[Out],
) *Runner[WArgs, W] {

	handler := newProcessorWorkerHandler(
		tel, stageMetrics, workerID, worker, messageReader, messageWriter,
	)

	return newRunner(tel, handler)
}

// NewEgressRunner returns a new runner for an egress worker.
func NewEgressRunner[WArgs any, In msgBody, W Egress[WArgs, In]](
	tel *telemetry.Telemetry, stageMetrics *metrics.EgressStage,
	workerID int, worker W,
	messageReader connector.MessageConnector[In],
) *Runner[WArgs, W] {

	handler := newEgressWorkerHandler(
		tel, stageMetrics, workerID, worker, messageReader,
	)

	return newRunner(tel, handler)
}

// Init initializes the worker.
func (r *Runner[WArgs, W]) Init(ctx context.Context, workerArgs WArgs) error {
	worker, workerID := r.workerHandler.getWorker()
	defer r.tel.LogDebug("worker initialized", "worker_id", workerID)

	worker.SetTelemetry(r.tel)

	if err := worker.Init(ctx, workerArgs); err != nil {
		r.tel.LogError("failed to init worker", err, "worker_id", workerID)
		return err
	}

	return nil
}

// Run runs the worker until the context is done.
// This method is meant to be used by single-threaded stages.
func (r *Runner[WArgs, W]) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			r.workerHandler.handle(ctx)
		}
	}
}

// RunPooled runs the worker until the context is done or the stopCh is closed.
// This method is meant to be used by pooled stages.
func (r *Runner[WArgs, W]) RunPooled(
	ctx context.Context, stopCh <-chan struct{}, pendingCounter *atomic.Int64,
) {

	for {
		select {
		case <-ctx.Done():
			return

		case <-stopCh:
			return

		default:
			pendingCounter.Add(1)
			r.workerHandler.handle(ctx)
			pendingCounter.Add(-1)
		}
	}
}

// Close closes the worker.
func (r *Runner[WArgs, W]) Close(ctx context.Context) error {
	worker, workerID := r.workerHandler.getWorker()
	defer r.tel.LogDebug("worker closed", "worker_id", workerID)

	if err := worker.Close(ctx); err != nil {
		r.tel.LogError("failed to close worker", err, "worker_id", workerID)
		return err
	}

	return nil
}
