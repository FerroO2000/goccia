package stage

import (
	"context"

	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
)

type stageRunnerBackend[Env env.Env, W worker.Worker[Env]] interface {
	setEnvironment(env Env)
	newWorkerExecutor(workerID int) *worker.Executor[Env, W]
	runInputBridge(ctx context.Context)
	runOutputBridge(ctx context.Context)
	closeOutput()
	inputConnectorID() uintptr
	outputConnectorID() uintptr
}

type processorRunnerBackend[Env env.Env, In, Out msgBody, W worker.Processor[Env, In, Out]] struct {
	env Env

	input  stageInput[In]
	output stageOutput[Out]

	workerMaker func() W
}

func newProcessorRunnerBackend[Env env.Env, In, Out msgBody, W worker.Processor[Env, In, Out]](
	input stageInput[In], output stageOutput[Out], workerMaker func() W,
) *processorRunnerBackend[Env, In, Out, W] {

	return &processorRunnerBackend[Env, In, Out, W]{
		input:  input,
		output: output,

		workerMaker: workerMaker,
	}
}

func (p *processorRunnerBackend[Env, In, Out, W]) setEnvironment(env Env) {
	p.env = env
}

func (p *processorRunnerBackend[Env, In, Out, W]) newWorkerExecutor(workerID int) *worker.Executor[Env, W] {
	w := p.workerMaker()
	reader := p.input.getWorkerExecutorReader()
	writer := p.output.getWorkerExecutorWriter()
	return worker.NewProcessorExecutor(p.env, workerID, w, reader, writer)
}

func (p *processorRunnerBackend[Env, In, Out, W]) runInputBridge(ctx context.Context) {
	p.input.run(ctx)
}

func (p *processorRunnerBackend[Env, In, Out, W]) runOutputBridge(ctx context.Context) {
	p.output.run(ctx)
}

func (p *processorRunnerBackend[Env, In, Out, W]) closeOutput() {
	p.output.close()
}

func (p *processorRunnerBackend[Env, In, Out, W]) inputConnectorID() uintptr {
	return p.input.getConnectorID()
}

func (p *processorRunnerBackend[Env, In, Out, W]) outputConnectorID() uintptr {
	return p.output.getConnectorID()
}

type egressRunnerBackend[Env env.Env, In msgBody, W worker.Egress[Env, In]] struct {
	env Env

	input stageInput[In]

	workerMaker func() W
}

func newEgressRunnerBackend[Env env.Env, In msgBody, W worker.Egress[Env, In]](
	input stageInput[In], workerMaker func() W,
) *egressRunnerBackend[Env, In, W] {

	return &egressRunnerBackend[Env, In, W]{
		input: input,

		workerMaker: workerMaker,
	}
}

func (ef *egressRunnerBackend[Env, In, W]) setEnvironment(env Env) {
	ef.env = env
}

func (ef *egressRunnerBackend[Env, In, W]) newWorkerExecutor(workerID int) *worker.Executor[Env, W] {
	w := ef.workerMaker()
	reader := ef.input.getWorkerExecutorReader()
	return worker.NewEgressExecutor(ef.env, workerID, w, reader)
}

func (ef *egressRunnerBackend[Env, In, W]) runInputBridge(ctx context.Context) {
	ef.input.run(ctx)
}

func (ef *egressRunnerBackend[Env, In, W]) runOutputBridge(_ context.Context) {}

func (ef *egressRunnerBackend[Env, In, W]) closeOutput() {}

func (ef *egressRunnerBackend[Env, In, W]) inputConnectorID() uintptr {
	return ef.input.getConnectorID()
}

func (ef *egressRunnerBackend[Env, In, W]) outputConnectorID() uintptr {
	return 0
}
