package worker

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/stage/env"
)

// Runner is a struct that represents a worker runner.
// It is used as a wrapper for the different kinds of worker,
// and it is meant to be used by both single-threaded and pooled stages.
type Runner[Env env.Env, W Worker[Env]] struct {
	env Env

	workerHandler workerHandler[Env, W]
}

func newRunner[Env env.Env, W Worker[Env]](env Env, workerHandler workerHandler[Env, W]) *Runner[Env, W] {
	return &Runner[Env, W]{
		env: env,

		workerHandler: workerHandler,
	}
}

// NewProcessorRunner returns a new runner for a processor worker.
func NewProcessorRunner[Env env.Env, In, Out msgBody, W Processor[Env, In, Out]](
	env Env, workerID int, worker W,
	messageReader connector.MessageConnector[In], messageWriter connector.MessageConnector[Out],
) *Runner[Env, W] {

	handler := newProcessorWorkerHandler(
		env.Telemetry(), env.GetProcessorMetrics(), workerID, worker, messageReader, messageWriter,
	)

	return newRunner(env, handler)
}

// NewEgressRunner returns a new runner for an egress worker.
func NewEgressRunner[Env env.Env, In msgBody, W Egress[Env, In]](
	env Env, workerID int, worker W,
	messageReader connector.MessageConnector[In],
) *Runner[Env, W] {

	handler := newEgressWorkerHandler(
		env.Telemetry(), env.GetEgressMetrics(), workerID, worker, messageReader,
	)

	return newRunner(env, handler)
}

// Init initializes the worker.
func (r *Runner[Env, W]) Init(ctx context.Context) error {
	worker, workerID := r.workerHandler.getWorker()
	defer r.env.Telemetry().LogDebug("worker initialized", "worker_id", workerID)

	worker.SetEnvironment(r.env)

	if err := worker.Init(ctx); err != nil {
		r.env.Telemetry().LogError("failed to init worker", err, "worker_id", workerID)
		return err
	}

	return nil
}

func (r *Runner[Env, W]) shallExitRun(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, connector.ErrClosed)
}

// Run runs the worker until the context is done.
// This method is meant to be used by single-threaded stages.
func (r *Runner[Env, W]) Run(ctx context.Context) {
	for {
		err := r.workerHandler.handle(ctx)
		if err != nil && r.shallExitRun(err) {
			return
		}
	}
}

// RunPooled runs the worker until the context is done or the stopCh is closed.
// This method is meant to be used by pooled stages.
func (r *Runner[Env, W]) RunPooled(
	ctx context.Context, stopCh <-chan struct{}, pendingCounter *atomic.Int64,
) {

	for {
		select {
		case <-stopCh:
			return

		default:
			pendingCounter.Add(1)
			err := r.workerHandler.handle(ctx)
			pendingCounter.Add(-1)

			if err != nil && r.shallExitRun(err) {
				return
			}
		}
	}
}

// Close closes the worker.
func (r *Runner[Env, W]) Close(ctx context.Context) error {
	worker, workerID := r.workerHandler.getWorker()
	defer r.env.Telemetry().LogDebug("worker closed", "worker_id", workerID)

	if err := worker.Close(ctx); err != nil {
		r.env.Telemetry().LogError("failed to close worker", err, "worker_id", workerID)
		return err
	}

	return nil
}
