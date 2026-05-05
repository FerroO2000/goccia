package stage

import (
	"context"
	"sync"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/pool"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/FerroO2000/goccia/internal/worker"
)

// Runner defines the interface for a stage runner.
type Runner[WArgs any, W worker.Worker[WArgs]] interface {
	Init(ctx context.Context) error
	Run(ctx context.Context)
	Close(ctx context.Context)
	Inputs() []uintptr
	Outputs() []uintptr
}

type baseRunner[WArgs any, W worker.Worker[WArgs]] struct {
	tel *telemetry.Telemetry

	workerArgs WArgs

	workerRunnerFactory stageWorkerRunnerFactory[WArgs, W]
}

func newBaseRunner[WArgs any, W worker.Worker[WArgs]](
	tel *telemetry.Telemetry,
	workerArgs WArgs, workerRunnerFactory stageWorkerRunnerFactory[WArgs, W],
) *baseRunner[WArgs, W] {

	return &baseRunner[WArgs, W]{
		tel: tel,

		workerArgs: workerArgs,

		workerRunnerFactory: workerRunnerFactory,
	}
}

// Inputs returns the input connector IDs.
func (br *baseRunner[WArgs, W]) Inputs() []uintptr {
	connID := br.workerRunnerFactory.getInputConnectorID()
	if connID != 0 {
		return []uintptr{connID}
	}

	return []uintptr{}
}

// Outputs returns the output connector IDs.
func (br *baseRunner[WArgs, W]) Outputs() []uintptr {
	connID := br.workerRunnerFactory.getOutputConnectorID()
	if connID != 0 {
		return []uintptr{connID}
	}

	return []uintptr{}
}

// ─── Single ───────────────────────────────────────────────────────────────────|

var _ Runner[any, worker.Worker[any]] = (*runnerSingle[any, worker.Worker[any]])(nil)

type runnerSingle[WArgs any, W worker.Worker[WArgs]] struct {
	*baseRunner[WArgs, W]

	workerRunner *worker.Runner[WArgs, W]
}

func newRunnerSingle[WArgs any, W worker.Worker[WArgs]](
	tel *telemetry.Telemetry,
	workerArgs WArgs, workerRunnerFactory stageWorkerRunnerFactory[WArgs, W],
) *runnerSingle[WArgs, W] {

	return &runnerSingle[WArgs, W]{
		baseRunner: newBaseRunner(tel, workerArgs, workerRunnerFactory),

		workerRunner: workerRunnerFactory.makeWorkerRunner(tel, 0),
	}
}

// Init initializes worker runner and the stage metrics.
func (rs *runnerSingle[WArgs, W]) Init(ctx context.Context) error {
	if err := rs.workerRunner.Init(ctx, rs.workerArgs); err != nil {
		return err
	}

	return rs.workerRunnerFactory.initMetrics(rs.tel)
}

// Run runs the worker runner.
func (rs *runnerSingle[WArgs, W]) Run(ctx context.Context) {
	rs.workerRunner.Run(ctx)
}

// Close closes the worker runner and the output connector (if any).
func (rs *runnerSingle[WArgs, W]) Close(ctx context.Context) {
	rs.workerRunner.Close(ctx)
	rs.workerRunnerFactory.closeIO()
}

// ─── Pool ───────────────────────────────────────────────────────────────────|

var _ Runner[any, worker.Worker[any]] = (*runnerPool[any, worker.Worker[any]])(nil)

type runnerPool[WArgs any, W worker.Worker[WArgs]] struct {
	*baseRunner[WArgs, W]

	initialWorkerRunner      int
	workerRunnerListenerDone chan struct{}
	workerRunnerWg           *sync.WaitGroup

	scaler *pool.Scaler
}

func newRunnerPool[WArgs any, W worker.Worker[WArgs]](
	tel *telemetry.Telemetry,
	workerArgs WArgs, workerRunnerFactory stageWorkerRunnerFactory[WArgs, W], cfg *config.Pool,
) *runnerPool[WArgs, W] {

	return &runnerPool[WArgs, W]{
		baseRunner: newBaseRunner(tel, workerArgs, workerRunnerFactory),

		initialWorkerRunner:      cfg.InitialWorkers,
		workerRunnerListenerDone: make(chan struct{}),
		workerRunnerWg:           &sync.WaitGroup{},

		scaler: pool.NewScaler(tel, cfg),
	}
}

// Init initializes the scaler and the stage metrics.
func (rp *runnerPool[WArgs, W]) Init(ctx context.Context) error {
	rp.scaler.Init(ctx, rp.initialWorkerRunner)
	return rp.workerRunnerFactory.initMetrics(rp.tel)
}

// runStartWorkerRunnerListener will trigger the creation of new worker runners
// when the scaler mandates.
func (rp *runnerPool[WArgs, W]) runStartWorkerRunnerListener(ctx context.Context) {
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
func (rp *runnerPool[WArgs, W]) startWorkerRunner(ctx context.Context) {
	defer rp.workerRunnerWg.Done()

	workerID := rp.scaler.NotifyWorkerStart()
	defer rp.scaler.NotifyWorkerStop()

	stopCh := rp.scaler.GetStopCh(workerID)
	if stopCh == nil {
		return
	}

	workerRunner := rp.workerRunnerFactory.makeWorkerRunner(rp.tel, workerID)

	if err := workerRunner.Init(ctx, rp.workerArgs); err != nil {
		return
	}
	defer workerRunner.Close(ctx)

	workerRunner.RunPooled(ctx, stopCh, rp.scaler.GetPendingCounter())
}

// Run runs the scaler, the bridge between the input/fan-out
// and/or the output/fan-in connectors, and the start worker runner listener.
func (rp *runnerPool[WArgs, W]) Run(ctx context.Context) {
	go rp.workerRunnerFactory.runIO(ctx)

	go rp.scaler.Run(ctx)
	rp.runStartWorkerRunnerListener(ctx)
}

// Close closes the scaler, the bridge between the input/fan-out
// and/or the output/fan-in connectors, and the output connector (if any).
func (rp *runnerPool[WArgs, W]) Close(_ context.Context) {
	<-rp.workerRunnerListenerDone
	rp.scaler.Close()

	rp.workerRunnerWg.Wait()

	rp.workerRunnerFactory.closeIO()
}
