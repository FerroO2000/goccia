package processor

import (
	"context"
	"errors"
	"sync"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/pool"
	"github.com/FerroO2000/goccia/internal/rb"
	stageCommon "github.com/FerroO2000/goccia/internal/stage"
)

type stage[WArgs any, In, Out msgEnv] interface {
	Init(ctx context.Context, workerArgs WArgs) error
	Run(ctx context.Context)
	Close()
}

func newStage[WArgs any, In, Out msgEnv](
	name string, inConn msgConn[In], outConn msgConn[Out], workerInstMaker workerInstanceMaker[WArgs, In, Out], cfg *stageCommon.Config,
) stage[WArgs, In, Out] {

	switch cfg.RunningMode {
	case stageCommon.RunningModeSingle:
		return newStageSingle(name, inConn, outConn, workerInstMaker)
	case stageCommon.RunningModePool:
		return newStagePool(name, inConn, outConn, workerInstMaker, cfg.Pool)
	default:
		return nil
	}
}

////////////
//  BASE  //
////////////

type stageBase[WArgs any, In, Out msgEnv] struct {
	tel *internal.Telemetry

	inputConnector  msgConn[In]
	outputConnector msgConn[Out]
}

func newStageBase[WArgs any, In, Out msgEnv](name string, inConn msgConn[In], outConn msgConn[Out]) *stageBase[WArgs, In, Out] {
	return &stageBase[WArgs, In, Out]{
		tel: internal.NewTelemetry("processor", name),

		inputConnector:  inConn,
		outputConnector: outConn,
	}
}

func (s *stageBase[WArgs, In, Out]) init() {
	s.tel.LogInfo("initializing")
}

func (s *stageBase[WArgs, In, Out]) run() {
	s.tel.LogInfo("running")
}

func (s *stageBase[WArgs, In, Out]) close() {
	s.tel.LogInfo("closing")

	// Close the output connector
	s.outputConnector.Close()
}

//////////////
//  SINGLE  //
//////////////

type stageSingle[WArgs any, In, Out msgEnv] struct {
	*stageBase[WArgs, In, Out]

	worker *worker[WArgs, In, Out]
}

func newStageSingle[WArgs any, In, Out msgEnv](
	name string, inConn msgConn[In], outConn msgConn[Out], workerInstMaker workerInstanceMaker[WArgs, In, Out],
) *stageSingle[WArgs, In, Out] {

	stageBase := newStageBase[WArgs](name, inConn, outConn)

	workerInst := workerInstMaker()
	workerMetrics := newWorkerMetrics(stageBase.tel)

	return &stageSingle[WArgs, In, Out]{
		stageBase: stageBase,

		worker: newWorker(stageBase.tel, 0, workerInst, workerMetrics),
	}
}

func (s *stageSingle[WArgs, In, Out]) Init(ctx context.Context, workerArgs WArgs) error {
	s.stageBase.init()

	// Initialize the worker metrics
	s.worker.metrics.init()

	return s.worker.init(ctx, workerArgs)
}

func (s *stageSingle[WArgs, In, Out]) Run(ctx context.Context) {
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

		if msgOut, valid := s.worker.process(ctx, msgIn); valid {
			// Write the message to the output connector
			if err := s.outputConnector.Write(msgOut); err != nil {
				msgOut.Destroy()
				s.tel.LogError("failed to write into output connector", err)
			}
		}
	}
}

func (s *stageSingle[WArgs, In, Out]) Close() {
	s.stageBase.close()

	s.worker.close(context.Background())
}

////////////
//  POOL  //
////////////

type stagePool[WArgs any, In, Out msgEnv] struct {
	*stageBase[WArgs, In, Out]

	writerWg   *sync.WaitGroup
	workerPool *workerPool[WArgs, In, Out]
}

func newStagePool[WArgs any, In, Out msgEnv](
	name string, inConn msgConn[In], outConn msgConn[Out], workerInstMaker workerInstanceMaker[WArgs, In, Out], cfg *pool.Config,
) *stagePool[WArgs, In, Out] {

	stageBase := newStageBase[WArgs](name, inConn, outConn)

	return &stagePool[WArgs, In, Out]{
		stageBase: newStageBase[WArgs](name, inConn, outConn),

		writerWg:   &sync.WaitGroup{},
		workerPool: newWorkerPool(stageBase.tel, workerInstMaker, cfg),
	}
}

func (s *stagePool[WArgs, In, Out]) Init(ctx context.Context, workerArgs WArgs) error {
	s.stageBase.init()

	return s.workerPool.init(ctx, workerArgs)
}

func (s *stagePool[WArgs, In, Out]) runWriter(ctx context.Context) {
	s.writerWg.Add(1)
	defer s.writerWg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgOut, err := s.workerPool.extractMessage()
		if err != nil {
			continue
		}

		if err := s.outputConnector.Write(msgOut); err != nil {
			s.tel.LogError("failed to write into output connector", err)
		}
	}
}

func (s *stagePool[WArgs, In, Out]) Run(ctx context.Context) {
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

		msg, err := s.inputConnector.Read()
		if err != nil {
			// Check if the input connector is closed, if so stop
			if errors.Is(err, connector.ErrClosed) {
				s.tel.LogInfo("input connector is closed, stopping")
				return
			}

			if !errors.Is(err, rb.ErrReadTimeout) {
				s.tel.LogError("failed to read from input connector", err)
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

func (s *stagePool[WArgs, In, Out]) Close() {
	s.stageBase.close()

	// Close the writer goroutine
	s.writerWg.Wait()

	s.workerPool.close()
}
