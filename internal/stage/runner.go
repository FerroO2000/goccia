package stage

import (
	"context"
	"sync"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/pool"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/FerroO2000/goccia/internal/worker"
)

type runner[WArgs any, W worker.Worker[WArgs]] interface {
	Init(ctx context.Context) error
	Run(ctx context.Context)
	Close(ctx context.Context)
}

type runnerSingle[WArgs any, W worker.Worker[WArgs]] struct {
	tel *telemetry.Telemetry

	workerArgs WArgs

	workerRunnerFactory stageWorkerRunnerFactory[WArgs, W]
	workerRunner        *worker.Runner[WArgs, W]
}

func newRunnerSingle[WArgs any, W worker.Worker[WArgs]](
	tel *telemetry.Telemetry,
	workerArgs WArgs, workerRunnerFactory stageWorkerRunnerFactory[WArgs, W],
) *runnerSingle[WArgs, W] {

	return &runnerSingle[WArgs, W]{
		tel: tel,

		workerArgs: workerArgs,

		workerRunnerFactory: workerRunnerFactory,
		workerRunner:        workerRunnerFactory.makeWorkerRunner(tel, 0),
	}
}

func (rs *runnerSingle[WArgs, W]) Init(ctx context.Context) error {
	if err := rs.workerRunner.Init(ctx, rs.workerArgs); err != nil {
		return err
	}

	return rs.workerRunnerFactory.initMetrics(rs.tel)
}

func (rs *runnerSingle[WArgs, W]) Run(ctx context.Context) {
	rs.workerRunner.Run(ctx)
}

func (rs *runnerSingle[WArgs, W]) Close(ctx context.Context) {
	rs.workerRunner.Close(ctx)
	rs.workerRunnerFactory.closeIO()
}

type runnerPool[WArgs any, W worker.Worker[WArgs]] struct {
	tel *telemetry.Telemetry

	workerArgs WArgs

	initialWorkerRunner      int
	workerRunnerListenerDone chan struct{}
	workerRunnerFactory      stageWorkerRunnerFactory[WArgs, W]
	workerRunnerWg           *sync.WaitGroup

	scaler *pool.Scaler
}

func newRunnerPool[WArgs any, W worker.Worker[WArgs]](
	tel *telemetry.Telemetry,
	workerArgs WArgs, workerRunnerFactory stageWorkerRunnerFactory[WArgs, W], cfg *config.Pool,
) *runnerPool[WArgs, W] {

	return &runnerPool[WArgs, W]{
		tel: tel,

		workerArgs: workerArgs,

		initialWorkerRunner:      cfg.InitialWorkers,
		workerRunnerListenerDone: make(chan struct{}),
		workerRunnerFactory:      workerRunnerFactory,
		workerRunnerWg:           &sync.WaitGroup{},

		scaler: pool.NewScaler(tel, cfg),
	}
}

func (rp *runnerPool[WArgs, W]) Init(ctx context.Context) error {
	rp.scaler.Init(ctx, rp.initialWorkerRunner)

	return rp.workerRunnerFactory.initMetrics(rp.tel)
}

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

func (rp *runnerPool[WArgs, W]) Run(ctx context.Context) {
	go rp.workerRunnerFactory.runIO(ctx)

	go rp.scaler.Run(ctx)
	rp.runStartWorkerRunnerListener(ctx)
}

func (rp *runnerPool[WArgs, W]) Close(_ context.Context) {
	rp.workerRunnerFactory.closeIO()

	<-rp.workerRunnerListenerDone

	rp.workerRunnerWg.Wait()
	rp.scaler.Close()
}
