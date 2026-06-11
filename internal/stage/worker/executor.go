package worker

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/stage/env"
)

// Executor executes a worker.
// It is used as a wrapper for the different kinds of worker,
// and it is meant to be used by both single-threaded and pooled stages.
type Executor[Env env.Env, W Worker[Env]] struct {
	env Env

	workerHandler workerHandler[Env, W]
}

func newExecutor[Env env.Env, W Worker[Env]](env Env, workerHandler workerHandler[Env, W]) *Executor[Env, W] {
	return &Executor[Env, W]{
		env: env,

		workerHandler: workerHandler,
	}
}

// NewProcessorExecutor returns a new executor for a processor worker.
func NewProcessorExecutor[Env env.Env, In, Out msgBody, W Processor[Env, In, Out]](
	env Env, workerID int, worker W,
	messageReader connector.MessageConnector[In], messageWriter connector.MessageConnector[Out],
) *Executor[Env, W] {

	handler := newProcessorWorkerHandler(
		env.Telemetry(), env.GetProcessorMetrics(), workerID, worker, messageReader, messageWriter,
	)

	return newExecutor(env, handler)
}

// NewEgressExecutor returns a new executor for an egress worker.
func NewEgressExecutor[Env env.Env, In msgBody, W Egress[Env, In]](
	env Env, workerID int, worker W,
	messageReader connector.MessageConnector[In],
) *Executor[Env, W] {

	handler := newEgressWorkerHandler(
		env.Telemetry(), env.GetEgressMetrics(), workerID, worker, messageReader,
	)

	return newExecutor(env, handler)
}

// Init initializes the worker.
func (e *Executor[Env, W]) Init(ctx context.Context) error {
	worker, workerID := e.workerHandler.getWorker()
	defer e.env.Telemetry().LogDebug("worker initialized", "worker_id", workerID)

	worker.SetEnvironment(e.env)

	if err := worker.Init(ctx); err != nil {
		e.env.Telemetry().LogError("failed to init worker", err, "worker_id", workerID)
		return err
	}

	return nil
}

func (e *Executor[Env, W]) shallExitRun(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, connector.ErrClosed)
}

// Run runs the worker until the context is done.
// This method is meant to be used by single-threaded stages.
func (e *Executor[Env, W]) Run(ctx context.Context) {
	for {
		err := e.workerHandler.handle(ctx)
		if err != nil && e.shallExitRun(err) {
			return
		}
	}
}

// RunPooled runs the worker until the context is done or the stopCh is closed.
// This method is meant to be used by pooled stages.
func (e *Executor[Env, W]) RunPooled(
	ctx context.Context, stopCh <-chan struct{}, pendingCounter *atomic.Int64,
) {

	for {
		select {
		case <-stopCh:
			return

		default:
			pendingCounter.Add(1)
			err := e.workerHandler.handle(ctx)
			pendingCounter.Add(-1)

			if err != nil && e.shallExitRun(err) {
				return
			}
		}
	}
}

// Close closes the worker.
func (e *Executor[Env, W]) Close(ctx context.Context) error {
	worker, workerID := e.workerHandler.getWorker()
	defer e.env.Telemetry().LogDebug("worker closed", "worker_id", workerID)

	if err := worker.Close(ctx); err != nil {
		e.env.Telemetry().LogError("failed to close worker", err, "worker_id", workerID)
		return err
	}

	return nil
}
