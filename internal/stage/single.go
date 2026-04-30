package stage

import (
	"context"
	"sync"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/pool"
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

	runnerWg      *sync.WaitGroup
	workerArgs    WArgs
	runnerFactory func() *worker.Runner[WArgs, W]
	scaler        *pool.Scaler
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

	runner := p.runnerFactory()

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
