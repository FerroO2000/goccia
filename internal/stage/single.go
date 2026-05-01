package stage

import (
	"context"
	"sync"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/pool"
	"github.com/FerroO2000/goccia/internal/rb"
	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/FerroO2000/goccia/internal/worker"
)

type Single[Cfg config.Config, WArgs any, W worker.Worker[WArgs]] struct {
	*BaseStage[Cfg]

	runner *worker.Runner[WArgs, W]
}

func (s *Single[Cfg, WArgs, W]) Init(ctx context.Context, workerArgs WArgs) error {
	return s.runner.Init(ctx, workerArgs)
}

func (s *Single[Cfg, WArgs, W]) Run(ctx context.Context) {
	s.runner.Run(ctx)
}

func (s *Single[Cfg, WArgs, W]) Close(ctx context.Context) {
	s.runner.Close(ctx)
}

type Pool[Cfg config.Config, WArgs any, W worker.Worker[WArgs]] struct {
	*BaseStage[Cfg]

	workerArgs    WArgs
	runnerFactory runnerFactory[WArgs, W]

	runnerWg *sync.WaitGroup
	scaler   *pool.Scaler
}

type runnerFactory[WArgs any, W worker.Worker[WArgs]] func(tel *telemetry.Telemetry, workerID int) *worker.Runner[WArgs, W]

func newPool[Cfg config.Config, WArgs any, W worker.Worker[WArgs]](
	kind Kind, name string, cfg Cfg, workerArgs WArgs, runnerFactory runnerFactory[WArgs, W],
) *Pool[Cfg, WArgs, W] {

	return &Pool[Cfg, WArgs, W]{
		BaseStage: newBaseStage(kind, name, cfg),

		workerArgs:    workerArgs,
		runnerFactory: runnerFactory,
	}
}

func (p *Pool[Cfg, WArgs, W]) runStartRunnerListener(ctx context.Context) {
	startCh := p.scaler.GetStartCh()

	for {
		select {
		case <-ctx.Done():
			return

		case <-startCh:
			p.runnerWg.Add(1)
			go p.startRunner(ctx)
		}
	}
}

func (p *Pool[Cfg, WArgs, W]) startRunner(ctx context.Context) {
	defer p.runnerWg.Done()

	workerID := p.scaler.NotifyWorkerStart()
	defer p.scaler.NotifyWorkerStop()

	stopCh := p.scaler.GetStopCh(workerID)
	if stopCh == nil {
		return
	}

	runner := p.runnerFactory(p.tel, workerID)

	if err := runner.Init(ctx, p.workerArgs); err != nil {
		return
	}
	defer runner.Close(ctx)

	runner.RunPooled(ctx, stopCh, p.scaler.GetPendingCounter())
}

func (p *Pool[Cfg, WArgs, W]) Run(ctx context.Context) {
	go p.scaler.Run(ctx)
	go p.runStartRunnerListener(ctx)
}

func newProcessorPool[Cfg config.WithStage, WArgs any, In, Out msgBody](
	name string, cfg Cfg, workerArgs WArgs, workerFactory func() worker.Processor[WArgs, In, Out],
) *Pool[Cfg, WArgs, worker.Processor[WArgs, In, Out]] {

	metrics := metrics.NewProcessorStage()

	fanOut := rb.NewRingBuffer[*msg[In]](512, rb.BufferKindSPMC)
	fanIn := rb.NewRingBuffer[*msg[Out]](512, rb.BufferKindSPMC)

	return newPool(
		KindProcessor, name, cfg, workerArgs,
		func(tel *telemetry.Telemetry, workerID int) *worker.Runner[WArgs, worker.Processor[WArgs, In, Out]] {
			return worker.NewProcessorRunner(tel, metrics, workerID, workerFactory(), fanOut, fanIn)
		},
	)
}
