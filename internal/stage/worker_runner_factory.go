package stage

import (
	"context"

	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
)

type stageWorkerRunnerFactory[Env env.Env, W worker.Worker[Env]] interface {
	setEnvironment(env Env)
	makeWorkerRunner(workerID int) *worker.Runner[Env, W]
	runInput(ctx context.Context)
	runOutput(ctx context.Context)
	closeOutput()
	getInputConnectorID() uintptr
	getOutputConnectorID() uintptr
}

type processorWorkerRunnerFactory[Env env.Env, In, Out msgBody, W worker.Processor[Env, In, Out]] struct {
	env Env

	input  stageInput[In]
	output stageOutput[Out]

	workerMaker func() W
}

func newProcessorWorkerRunnerFactory[Env env.Env, In, Out msgBody, W worker.Processor[Env, In, Out]](
	input stageInput[In], output stageOutput[Out], workerMaker func() W,
) *processorWorkerRunnerFactory[Env, In, Out, W] {

	return &processorWorkerRunnerFactory[Env, In, Out, W]{
		input:  input,
		output: output,

		workerMaker: workerMaker,
	}
}

func (p *processorWorkerRunnerFactory[Env, In, Out, W]) setEnvironment(env Env) {
	p.env = env
}

func (p *processorWorkerRunnerFactory[Env, In, Out, W]) makeWorkerRunner(workerID int) *worker.Runner[Env, W] {
	w := p.workerMaker()
	reader := p.input.getWorkerRunnerReader()
	writer := p.output.getWorkerRunnerWriter()
	return worker.NewProcessorRunner(p.env, workerID, w, reader, writer)
}

func (p *processorWorkerRunnerFactory[Env, In, Out, W]) runInput(ctx context.Context) {
	p.input.run(ctx)
}

func (p *processorWorkerRunnerFactory[Env, In, Out, W]) runOutput(ctx context.Context) {
	p.output.run(ctx)
}

func (p *processorWorkerRunnerFactory[Env, In, Out, W]) closeOutput() {
	p.output.close()
}

func (p *processorWorkerRunnerFactory[Env, In, Out, W]) getInputConnectorID() uintptr {
	return p.input.getConnectorID()
}

func (p *processorWorkerRunnerFactory[Env, In, Out, W]) getOutputConnectorID() uintptr {
	return p.output.getConnectorID()
}

type egressWorkerRunnerFactory[Env env.Env, In msgBody, W worker.Egress[Env, In]] struct {
	env Env

	input stageInput[In]

	workerMaker func() W
}

func newEgressWorkerRunnerFactory[Env env.Env, In msgBody, W worker.Egress[Env, In]](
	input stageInput[In], workerMaker func() W,
) *egressWorkerRunnerFactory[Env, In, W] {

	return &egressWorkerRunnerFactory[Env, In, W]{
		input: input,

		workerMaker: workerMaker,
	}
}

func (ef *egressWorkerRunnerFactory[Env, In, W]) setEnvironment(env Env) {
	ef.env = env
}

func (ef *egressWorkerRunnerFactory[Env, In, W]) makeWorkerRunner(workerID int) *worker.Runner[Env, W] {
	w := ef.workerMaker()
	reader := ef.input.getWorkerRunnerReader()
	return worker.NewEgressRunner(ef.env, workerID, w, reader)
}

func (ef *egressWorkerRunnerFactory[Env, In, W]) runInput(ctx context.Context) {
	ef.input.run(ctx)
}

func (ef *egressWorkerRunnerFactory[Env, In, W]) runOutput(_ context.Context) {}

func (ef *egressWorkerRunnerFactory[Env, In, W]) closeOutput() {}

func (ef *egressWorkerRunnerFactory[Env, In, W]) getInputConnectorID() uintptr {
	return ef.input.getConnectorID()
}

func (ef *egressWorkerRunnerFactory[Env, In, W]) getOutputConnectorID() uintptr {
	return 0
}
