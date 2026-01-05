package egress

import (
	"context"
	"errors"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
)

type stage[WArgs any, In msgBody, Cfg cfg] interface {
	Init(ctx context.Context, workerArgs WArgs) error
	Run(ctx context.Context)
	Close()
	Tel() *internal.Telemetry
	Config() Cfg
}

func newStage[WArgs any, In msgBody, Cfg stageCfg](
	name string, inConn msgConn[In], workerInstMaker workerInstanceMaker[WArgs, In], cfg Cfg,
) stage[WArgs, In, Cfg] {

	stageCfg := cfg.GetStage()

	switch stageCfg.RunningMode {
	case config.StageRunningModeSingle:
		return newStageSingle(name, inConn, workerInstMaker, cfg)
	case config.StageRunningModePool:
		return newStagePool(name, inConn, workerInstMaker, cfg, stageCfg.Pool)
	default:
		return nil
	}
}

////////////
//  BASE  //
////////////

type stageBase[WArgs any, In msgBody, Cfg cfg] struct {
	tel *internal.Telemetry

	config Cfg

	inputConnector msgConn[In]
}

func newStageBase[WArgs any, In msgBody, Cfg cfg](name string, inConn msgConn[In], cfg Cfg) *stageBase[WArgs, In, Cfg] {
	return &stageBase[WArgs, In, Cfg]{
		tel: internal.NewTelemetry("egress", name),

		config: cfg,

		inputConnector: inConn,
	}
}

func (s *stageBase[WArgs, In, Cfg]) init() {
	s.tel.LogInfo("initializing")

	configValidator := config.NewValidator(s.tel)
	configValidator.Validate(s.config)
}

func (s *stageBase[WArgs, In, Cfg]) run() {
	s.tel.LogInfo("running")
}

func (s *stageBase[WArgs, In, Cfg]) close() {
	s.tel.LogInfo("closing")
}

func (s *stageBase[WArgs, In, Cfg]) Tel() *internal.Telemetry {
	return s.tel
}

func (s *stageBase[WArgs, In, Cfg]) Config() Cfg {
	return s.config
}

//////////////
//  SINGLE  //
//////////////

type stageSingle[WArgs any, In msgBody, Cfg cfg] struct {
	*stageBase[WArgs, In, Cfg]

	worker *worker[WArgs, In]
}

func newStageSingle[WArgs any, In msgBody, Cfg cfg](
	name string, inConn msgConn[In], workerInstMaker workerInstanceMaker[WArgs, In], cfg Cfg,
) *stageSingle[WArgs, In, Cfg] {

	stageBase := newStageBase[WArgs](name, inConn, cfg)

	workerInst := workerInstMaker()
	workerMetrics := newWorkerMetrics(stageBase.tel)

	return &stageSingle[WArgs, In, Cfg]{
		stageBase: stageBase,

		worker: newWorker(stageBase.tel, 0, workerInst, workerMetrics),
	}
}

func (s *stageSingle[WArgs, In, Cfg]) Init(ctx context.Context, workerArgs WArgs) error {
	s.stageBase.init()

	// Initialize the worker metrics
	s.worker.metrics.init()

	return s.worker.init(ctx, workerArgs)
}

func (s *stageSingle[WArgs, In, Cfg]) Run(ctx context.Context) {
	s.stageBase.run()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgIn, err := s.inputConnector.Read(ctx)
		if err != nil {
			// Check if the input connector is closed, if so stop
			if errors.Is(err, connector.ErrClosed) {
				s.tel.LogInfo("input connector is closed, stopping")
				return
			}

			continue
		}

		s.worker.deliver(ctx, msgIn)
	}
}

func (s *stageSingle[WArgs, In, Cfg]) Close() {
	s.stageBase.close()

	s.worker.close(context.Background())
}

////////////
//  POOL  //
////////////

type stagePool[WArgs any, In msgBody, Cfg cfg] struct {
	*stageBase[WArgs, In, Cfg]

	workerPool *workerPool[WArgs, In]
}

func newStagePool[WArgs any, In msgBody, Cfg cfg](
	name string, inConn msgConn[In], workerInstMaker workerInstanceMaker[WArgs, In], cfg Cfg, poolCfg *config.Pool,
) *stagePool[WArgs, In, Cfg] {

	stageBase := newStageBase[WArgs](name, inConn, cfg)

	return &stagePool[WArgs, In, Cfg]{
		stageBase: stageBase,

		workerPool: newWorkerPool(stageBase.tel, workerInstMaker, poolCfg),
	}
}

func (s *stagePool[WArgs, In, Cfg]) Init(ctx context.Context, workerArgs WArgs) error {
	s.stageBase.init()

	return s.workerPool.init(ctx, workerArgs)
}

func (s *stagePool[WArgs, In, Cfg]) Run(ctx context.Context) {
	s.stageBase.run()

	// Run the worker pool
	go s.workerPool.run(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		default:
		}

		msg, err := s.inputConnector.Read(ctx)
		if err != nil {
			// Check if the input connector is closed, if so stop
			if errors.Is(err, connector.ErrClosed) {
				s.tel.LogInfo("input connector is closed, stopping")
				return
			}

			continue
		}

		s.workerPool.addMessage(ctx, msg)
	}
}

func (s *stagePool[WArgs, In, Cfg]) Close() {
	s.stageBase.close()

	s.workerPool.close()
}
