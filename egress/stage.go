package egress

import (
	"context"
	"errors"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/pool"
	stageCommon "github.com/FerroO2000/goccia/internal/stage"
)

type stage[WArgs any, In msgEnv] interface {
	Init(ctx context.Context, workerArgs WArgs) error
	Run(ctx context.Context)
	Close()
	Tel() *internal.Telemetry
}

func newStage[WArgs any, In msgEnv](
	name string, inConn msgConn[In], workerInstMaker workerInstanceMaker[WArgs, In], cfg *stageCommon.Config,
) stage[WArgs, In] {

	switch cfg.RunningMode {
	case stageCommon.RunningModeSingle:
		return newStageSingle(name, inConn, workerInstMaker)
	case stageCommon.RunningModePool:
		return newStagePool(name, inConn, workerInstMaker, cfg.Pool)
	default:
		return nil
	}
}

////////////
//  BASE  //
////////////

type stageBase[WArgs any, In msgEnv] struct {
	tel *internal.Telemetry

	inputConnector msgConn[In]
}

func newStageBase[WArgs any, In msgEnv](name string, inConn msgConn[In]) *stageBase[WArgs, In] {
	return &stageBase[WArgs, In]{
		tel: internal.NewTelemetry("egress", name),

		inputConnector: inConn,
	}
}

func (s *stageBase[WArgs, In]) init() {
	s.tel.LogInfo("initializing")
}

func (s *stageBase[WArgs, In]) run() {
	s.tel.LogInfo("running")
}

func (s *stageBase[WArgs, In]) close() {
	s.tel.LogInfo("closing")
}

func (s *stageBase[WArgs, In]) Tel() *internal.Telemetry {
	return s.tel
}

//////////////
//  SINGLE  //
//////////////

type stageSingle[WArgs any, In msgEnv] struct {
	*stageBase[WArgs, In]

	worker *worker[WArgs, In]
}

func newStageSingle[WArgs any, In msgEnv](
	name string, inConn msgConn[In], workerInstMaker workerInstanceMaker[WArgs, In],
) *stageSingle[WArgs, In] {

	stageBase := newStageBase[WArgs](name, inConn)

	workerInst := workerInstMaker()
	workerMetrics := newWorkerMetrics(stageBase.tel)

	return &stageSingle[WArgs, In]{
		stageBase: stageBase,

		worker: newWorker(stageBase.tel, 0, workerInst, workerMetrics),
	}
}

func (s *stageSingle[WArgs, In]) Init(ctx context.Context, workerArgs WArgs) error {
	s.stageBase.init()

	// Initialize the worker metrics
	s.worker.metrics.init()

	return s.worker.init(ctx, workerArgs)
}

func (s *stageSingle[WArgs, In]) Run(ctx context.Context) {
	s.stageBase.run()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgIn, err := s.inputConnector.Read()
		if err != nil {
			// Check if the input connector is closed, if so stop
			if errors.Is(err, connector.ErrClosed) {
				s.tel.LogInfo("input connector is closed, stopping")
				return
			}

			if !errors.Is(err, connector.ErrReadTimeout) {
				s.tel.LogError("failed to read from input connector", err)
			}

			continue
		}

		s.worker.deliver(ctx, msgIn)
	}
}

func (s *stageSingle[WArgs, In]) Close() {
	s.stageBase.close()

	s.worker.close(context.Background())
}

////////////
//  POOL  //
////////////

type stagePool[WArgs any, In msgEnv] struct {
	*stageBase[WArgs, In]

	workerPool *workerPool[WArgs, In]
}

func newStagePool[WArgs any, In msgEnv](
	name string, inConn msgConn[In], workerInstMaker workerInstanceMaker[WArgs, In], cfg *pool.Config,
) *stagePool[WArgs, In] {

	stageBase := newStageBase[WArgs](name, inConn)

	return &stagePool[WArgs, In]{
		stageBase: stageBase,

		workerPool: newWorkerPool(stageBase.tel, workerInstMaker, cfg),
	}
}

func (s *stagePool[WArgs, In]) Init(ctx context.Context, workerArgs WArgs) error {
	s.stageBase.init()

	return s.workerPool.init(ctx, workerArgs)
}

func (s *stagePool[WArgs, In]) Run(ctx context.Context) {
	s.stageBase.run()

	// Run the worker pool
	go s.workerPool.run(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		default:
		}

		msg, err := s.inputConnector.Read()
		if err != nil {
			// Check if the input connector is closed, if so stop
			if errors.Is(err, connector.ErrClosed) {
				s.tel.LogInfo("input connector is closed, stopping")
				return
			}

			if !errors.Is(err, connector.ErrReadTimeout) {
				s.tel.LogError("failed to read from input connector", err)
			}

			continue
		}

		s.workerPool.addMessage(ctx, msg)
	}
}

func (s *stagePool[WArgs, In]) Close() {
	s.stageBase.close()

	s.workerPool.close()
}
