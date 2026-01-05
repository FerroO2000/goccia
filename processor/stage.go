package processor

import (
	"context"
	"errors"
	"sync"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
)

type stage[WArgs any, In, Out msgBody, Cfg cfg] interface {
	Init(ctx context.Context, workerArgs WArgs) error
	Run(ctx context.Context)
	Close()
	Config() Cfg
}

func newStage[WArgs any, In, Out msgBody, Cfg stageCfg](
	name string, inConn msgConn[In], outConn msgConn[Out], workerInstMaker workerInstanceMaker[WArgs, In, Out], cfg Cfg,
) stage[WArgs, In, Out, Cfg] {

	stageCfg := cfg.GetStage()

	switch stageCfg.RunningMode {
	case config.StageRunningModeSingle:
		return newStageSingle(name, inConn, outConn, workerInstMaker, cfg)
	case config.StageRunningModePool:
		return newStagePool(name, inConn, outConn, workerInstMaker, cfg, stageCfg.Pool)
	default:
		return nil
	}
}

////////////
//  BASE  //
////////////

type stageBase[WArgs any, In, Out msgBody, Cfg cfg] struct {
	tel *internal.Telemetry

	config Cfg

	inputConnector  msgConn[In]
	outputConnector msgConn[Out]
}

func newStageBase[WArgs any, In, Out msgBody, Cfg cfg](
	name string, inConn msgConn[In], outConn msgConn[Out], cfg Cfg) *stageBase[WArgs, In, Out, Cfg] {

	return &stageBase[WArgs, In, Out, Cfg]{
		tel: internal.NewTelemetry("processor", name),

		config: cfg,

		inputConnector:  inConn,
		outputConnector: outConn,
	}
}

func (s *stageBase[WArgs, In, Out, Cfg]) init() {
	s.tel.LogInfo("initializing")

	configValidator := config.NewValidator(s.tel)
	configValidator.Validate(s.config)
}

func (s *stageBase[WArgs, In, Out, Cfg]) run() {
	s.tel.LogInfo("running")
}

func (s *stageBase[WArgs, In, Out, Cfg]) close() {
	s.tel.LogInfo("closing")

	// Close the output connector
	s.outputConnector.Close()
}

func (s *stageBase[WArgs, In, Out, Cfg]) Config() Cfg {
	return s.config
}

//////////////
//  SINGLE  //
//////////////

type stageSingle[WArgs any, In, Out msgBody, Cfg cfg] struct {
	*stageBase[WArgs, In, Out, Cfg]

	worker *worker[WArgs, In, Out]
}

func newStageSingle[WArgs any, In, Out msgBody, Cfg cfg](
	name string, inConn msgConn[In], outConn msgConn[Out], workerInstMaker workerInstanceMaker[WArgs, In, Out], cfg Cfg,
) *stageSingle[WArgs, In, Out, Cfg] {

	stageBase := newStageBase[WArgs](name, inConn, outConn, cfg)

	workerInst := workerInstMaker()
	workerMetrics := newWorkerMetrics(stageBase.tel)

	return &stageSingle[WArgs, In, Out, Cfg]{
		stageBase: stageBase,

		worker: newWorker(stageBase.tel, 0, workerInst, workerMetrics),
	}
}

func (s *stageSingle[WArgs, In, Out, Cfg]) Init(ctx context.Context, workerArgs WArgs) error {
	s.stageBase.init()

	// Initialize the worker metrics
	s.worker.metrics.init()

	return s.worker.init(ctx, workerArgs)
}

func (s *stageSingle[WArgs, In, Out, Cfg]) Run(ctx context.Context) {
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

		if msgOut, valid := s.worker.process(ctx, msgIn); valid {
			// Write the message to the output connector
			if err := s.outputConnector.Write(msgOut); err != nil {
				msgOut.Destroy()
				s.tel.LogError("failed to write into output connector", err)
			}
		}
	}
}

func (s *stageSingle[WArgs, In, Out, Cfg]) Close() {
	s.stageBase.close()

	s.worker.close(context.Background())
}

////////////
//  POOL  //
////////////

type stagePool[WArgs any, In, Out msgBody, Cfg cfg] struct {
	*stageBase[WArgs, In, Out, Cfg]

	writerWg   *sync.WaitGroup
	workerPool *workerPool[WArgs, In, Out]
}

func newStagePool[WArgs any, In, Out msgBody, Cfg cfg](
	name string, inConn msgConn[In], outConn msgConn[Out], workerInstMaker workerInstanceMaker[WArgs, In, Out], cfg Cfg, poolCfg *config.Pool,
) *stagePool[WArgs, In, Out, Cfg] {

	stageBase := newStageBase[WArgs](name, inConn, outConn, cfg)

	return &stagePool[WArgs, In, Out, Cfg]{
		stageBase: newStageBase[WArgs](name, inConn, outConn, cfg),

		writerWg:   &sync.WaitGroup{},
		workerPool: newWorkerPool(stageBase.tel, workerInstMaker, poolCfg),
	}
}

func (s *stagePool[WArgs, In, Out, Cfg]) Init(ctx context.Context, workerArgs WArgs) error {
	s.stageBase.init()

	return s.workerPool.init(ctx, workerArgs)
}

func (s *stagePool[WArgs, In, Out, Cfg]) runWriter(ctx context.Context) {
	s.writerWg.Add(1)
	defer s.writerWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgOut, err := s.workerPool.extractMessage(ctx)
		if err != nil {
			continue
		}

		if err := s.outputConnector.Write(msgOut); err != nil {
			s.tel.LogError("failed to write into output connector", err)
		}
	}
}

func (s *stagePool[WArgs, In, Out, Cfg]) Run(ctx context.Context) {
	s.stageBase.run()

	// Run the worker pool
	go s.workerPool.run(ctx)

	// Run the writer goroutine
	go s.runWriter(ctx)

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

		// Push a new task to the worker pool
		if err := s.workerPool.addMessage(ctx, msg); err != nil {
			s.tel.LogError("failed to add message to worker pool", err)
			continue
		}
	}
}

func (s *stagePool[WArgs, In, Out, Cfg]) Close() {
	s.stageBase.close()

	// Close the writer goroutine
	s.writerWg.Wait()

	s.workerPool.close()
}
