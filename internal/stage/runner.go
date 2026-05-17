package stage

import (
	"context"
	"sync"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/pool"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
)

// Runner defines the interface for a stage runner.
type Runner[Env env.Env] interface {
	SetEnvironment(env Env)
	Init(ctx context.Context) error
	Run(ctx context.Context)
	Close(ctx context.Context)
	Inputs() []uintptr
	Outputs() []uintptr
}

func newRunner[Env env.Env, W worker.Worker[Env]](
	workerRunnerFactory stageWorkerRunnerFactory[Env, W],
	cfg *config.Stage,
) Runner[Env] {

	switch cfg.RunningMode {
	case config.StageRunningModeSingle:
		return newRunnerSingle(workerRunnerFactory)

	case config.StageRunningModePool:
		return newRunnerPool(workerRunnerFactory, cfg.Pool)

	default:
		panic("invalid running mode")
	}
}

// ─── Base ───────────────────────────────────────────────────────────────────|

type baseRunner[Env env.Env, W worker.Worker[Env]] struct {
	workerRunnerFactory stageWorkerRunnerFactory[Env, W]
}

func newBaseRunner[Env env.Env, W worker.Worker[Env]](
	workerRunnerFactory stageWorkerRunnerFactory[Env, W],
) *baseRunner[Env, W] {

	return &baseRunner[Env, W]{
		workerRunnerFactory: workerRunnerFactory,
	}
}

func (br *baseRunner[Env, W]) SetEnvironment(env Env) {
	br.workerRunnerFactory.setEnvironment(env)
}

// Inputs returns the input connector IDs.
func (br *baseRunner[Env, W]) Inputs() []uintptr {
	connID := br.workerRunnerFactory.getInputConnectorID()
	if connID != 0 {
		return []uintptr{connID}
	}

	return []uintptr{}
}

// Outputs returns the output connector IDs.
func (br *baseRunner[Env, W]) Outputs() []uintptr {
	connID := br.workerRunnerFactory.getOutputConnectorID()
	if connID != 0 {
		return []uintptr{connID}
	}

	return []uintptr{}
}

// ─── Single ─────────────────────────────────────────────────────────────────|

var _ Runner[env.Env] = (*runnerSingle[env.Env, worker.Worker[env.Env]])(nil)

type runnerSingle[Env env.Env, W worker.Worker[Env]] struct {
	*baseRunner[Env, W]

	workerRunner *worker.Runner[Env, W]
}

func newRunnerSingle[Env env.Env, W worker.Worker[Env]](
	workerRunnerFactory stageWorkerRunnerFactory[Env, W],
) *runnerSingle[Env, W] {

	return &runnerSingle[Env, W]{
		baseRunner: newBaseRunner(workerRunnerFactory),

		workerRunner: workerRunnerFactory.makeWorkerRunner(0),
	}
}

// Init initializes worker runner and the stage metrics.
func (rs *runnerSingle[Env, W]) Init(ctx context.Context) error {
	return rs.workerRunner.Init(ctx)
}

// Run runs the worker runner.
func (rs *runnerSingle[Env, W]) Run(ctx context.Context) {
	rs.workerRunner.Run(ctx)
}

// Close closes the worker runner and the output connector (if any).
func (rs *runnerSingle[Env, W]) Close(ctx context.Context) {
	rs.workerRunner.Close(ctx)
	rs.workerRunnerFactory.closeIO()
}

// ─── Pool ───────────────────────────────────────────────────────────────────|

var _ Runner[env.Env] = (*runnerPool[env.Env, worker.Worker[env.Env]])(nil)

type runnerPool[Env env.Env, W worker.Worker[Env]] struct {
	*baseRunner[Env, W]

	initArgs Env

	initialWorkerRunner      int
	workerRunnerListenerDone chan struct{}
	workerRunnerWg           *sync.WaitGroup

	scaler *pool.Scaler
}

func newRunnerPool[Env env.Env, W worker.Worker[Env]](
	workerRunnerFactory stageWorkerRunnerFactory[Env, W],
	cfg *config.Pool,
) *runnerPool[Env, W] {

	return &runnerPool[Env, W]{
		baseRunner: newBaseRunner(workerRunnerFactory),

		initialWorkerRunner:      cfg.InitialWorkers,
		workerRunnerListenerDone: make(chan struct{}),
		workerRunnerWg:           &sync.WaitGroup{},

		scaler: pool.NewScaler(nil, cfg),
	}
}

func (rp *runnerPool[Env, W]) SetEnvironment(env Env) {
	rp.baseRunner.SetEnvironment(env)
	rp.scaler.SetTelemetry(env.Telemetry())
}

// Init initializes the scaler and the stage metrics.
func (rp *runnerPool[Env, W]) Init(ctx context.Context) error {
	rp.scaler.Init(ctx, rp.initialWorkerRunner)
	return nil
}

// runStartWorkerRunnerListener will trigger the creation of new worker runners
// when the scaler mandates.
func (rp *runnerPool[Env, W]) runStartWorkerRunnerListener(ctx context.Context) {
	defer close(rp.workerRunnerListenerDone)
	startCh := rp.scaler.GetStartCh()

	for {
		select {
		case <-ctx.Done():
			return

		case <-startCh:
			rp.workerRunnerWg.Add(1)
			go rp.startWorkerRunner(ctx)
		}
	}
}

// startWorkerRunner creates a new worker runner and starts it.
// It will go through the full lifecycle of a worker runner (Init, Run, Close).
func (rp *runnerPool[Env, W]) startWorkerRunner(ctx context.Context) {
	defer rp.workerRunnerWg.Done()

	workerID := rp.scaler.NotifyWorkerStart()
	defer rp.scaler.NotifyWorkerStop()

	stopCh := rp.scaler.GetStopCh(workerID)
	if stopCh == nil {
		return
	}

	workerRunner := rp.workerRunnerFactory.makeWorkerRunner(workerID)

	if err := workerRunner.Init(ctx); err != nil {
		return
	}
	defer workerRunner.Close(ctx)

	workerRunner.RunPooled(ctx, stopCh, rp.scaler.GetPendingCounter())
}

// Run runs the scaler, the bridge between the input/fan-out
// and/or the output/fan-in connectors, and the start worker runner listener.
func (rp *runnerPool[Env, W]) Run(ctx context.Context) {
	go rp.workerRunnerFactory.runIO(ctx)

	go rp.scaler.Run(ctx)
	rp.runStartWorkerRunnerListener(ctx)
}

// Close closes the scaler, the bridge between the input/fan-out
// and/or the output/fan-in connectors, and the output connector (if any).
func (rp *runnerPool[Env, W]) Close(_ context.Context) {
	<-rp.workerRunnerListenerDone
	rp.scaler.Close()

	rp.workerRunnerWg.Wait()

	rp.workerRunnerFactory.closeIO()
}
