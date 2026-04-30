package worker

import (
	"context"
	"sync/atomic"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

type runnerHandler[WArgs any, W Worker[WArgs]] interface {
	handle(ctx context.Context)
}

// Runner is a struct that represents a worker runner.
// It is used as a wrapper for the different kinds of worker,
// and it is meant to be used by both single-threaded and pooled stages.
type Runner[WArgs any, W Worker[WArgs]] struct {
	tel *telemetry.Telemetry

	workerID int
	worker   W

	handler runnerHandler[WArgs, W]
}

func newRunner[WArgs any, W Worker[WArgs]](
	tel *telemetry.Telemetry, workerID int, worker W, handler runnerHandler[WArgs, W],
) *Runner[WArgs, W] {

	return &Runner[WArgs, W]{
		tel: tel,

		workerID: workerID,
		worker:   worker,

		handler: handler,
	}
}

// NewProcessorRunner returns a new runner for a processor worker.
func NewProcessorRunner[WArgs any, In, Out msgBody](
	tel *telemetry.Telemetry, metrics *metrics.ProcessorStage,
	workerID int, worker Processor[WArgs, In, Out],
	messageReader connector.MessageConnector[In], messageWriter connector.MessageConnector[Out],
) *Runner[WArgs, Processor[WArgs, In, Out]] {

	handler := newProcessorRunnerHandler(
		tel, metrics, workerID, worker, messageReader, messageWriter,
	)

	return newRunner(tel, workerID, worker, handler)
}

// NewEgressRunner returns a new runner for an egress worker.
func NewEgressRunner[WArgs any, In msgBody](
	tel *telemetry.Telemetry, metrics *metrics.EgressStage,
	workerID int, worker Egress[WArgs, In],
	messageReader connector.MessageConnector[In],
) *Runner[WArgs, Egress[WArgs, In]] {

	handler := newEgressRunnerHandler(
		tel, metrics, workerID, worker, messageReader,
	)

	return newRunner(tel, workerID, worker, handler)
}

// Init initializes the worker.
func (r *Runner[WArgs, W]) Init(ctx context.Context, workerArgs WArgs) error {
	defer r.tel.LogDebug("worker initialized", "worker_id", r.workerID)

	r.worker.SetTelemetry(r.tel)

	if err := r.worker.Init(ctx, workerArgs); err != nil {
		r.tel.LogError("failed to init worker", err, "worker_id", r.workerID)
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
			r.handler.handle(ctx)
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
			r.handler.handle(ctx)
			pendingCounter.Add(-1)
		}
	}
}

// Close closes the worker.
func (r *Runner[WArgs, W]) Close(ctx context.Context) error {
	defer r.tel.LogDebug("worker closed", "worker_id", r.workerID)

	if err := r.worker.Close(ctx); err != nil {
		r.tel.LogError("failed to close worker", err, "worker_id", r.workerID)
		return err
	}

	return nil
}
