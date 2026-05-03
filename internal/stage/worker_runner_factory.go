package stage

import (
	"context"

	"github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/FerroO2000/goccia/internal/worker"
)

type stageWorkerRunnerFactory[WArgs any, W worker.Worker[WArgs]] interface {
	initMetrics(tel *telemetry.Telemetry) error
	makeWorkerRunner(tel *telemetry.Telemetry, workerID int) *worker.Runner[WArgs, W]
	runIO(ctx context.Context)
	closeIO()
}

type processorWorkerRunnerFactory[WArgs any, In, Out msgBody, W worker.Processor[WArgs, In, Out]] struct {
	input  stageInput[In]
	output stageOutput[Out]

	workerMaker func() W

	metrics *metrics.ProcessorStage
}

func newProcessorWorkerRunnerFactory[WArgs any, In, Out msgBody, W worker.Processor[WArgs, In, Out]](
	input stageInput[In], output stageOutput[Out],
	workerMaker func() W,
	metrics *metrics.ProcessorStage,
) *processorWorkerRunnerFactory[WArgs, In, Out, W] {

	return &processorWorkerRunnerFactory[WArgs, In, Out, W]{
		input:  input,
		output: output,

		workerMaker: workerMaker,

		metrics: metrics,
	}
}

func (p *processorWorkerRunnerFactory[WArgs, In, Out, W]) initMetrics(tel *telemetry.Telemetry) error {
	return p.metrics.InitMetrics(tel)
}

func (p *processorWorkerRunnerFactory[WArgs, In, Out, W]) makeWorkerRunner(tel *telemetry.Telemetry, workerID int) *worker.Runner[WArgs, W] {
	w := p.workerMaker()
	reader := p.input.getWorkerRunnerReader()
	writer := p.output.getWorkerRunnerWriter()
	return worker.NewProcessorRunner(tel, p.metrics, workerID, w, reader, writer)
}

func (p *processorWorkerRunnerFactory[WArgs, In, Out, W]) runIO(ctx context.Context) {
	p.input.run(ctx)
	p.output.run(ctx)
}

func (p *processorWorkerRunnerFactory[WArgs, In, Out, W]) closeIO() {
	p.output.close()
}

type egressWorkerRunnerFactory[WArgs any, In msgBody, W worker.Egress[WArgs, In]] struct {
	input stageInput[In]

	workerMaker func() W

	metrics *metrics.EgressStage
}

func newEgressWorkerRunnerFactory[WArgs any, In msgBody, W worker.Egress[WArgs, In]](
	input stageInput[In],
	workerMaker func() W,
	metrics *metrics.EgressStage,
) *egressWorkerRunnerFactory[WArgs, In, W] {

	return &egressWorkerRunnerFactory[WArgs, In, W]{
		input: input,

		workerMaker: workerMaker,

		metrics: metrics,
	}
}

func (p *egressWorkerRunnerFactory[WArgs, In, W]) initMetrics(tel *telemetry.Telemetry) error {
	return p.metrics.InitMetrics(tel)
}

func (ef *egressWorkerRunnerFactory[WArgs, In, W]) makeWorkerRunner(tel *telemetry.Telemetry, workerID int) *worker.Runner[WArgs, W] {
	w := ef.workerMaker()
	reader := ef.input.getWorkerRunnerReader()
	return worker.NewEgressRunner(tel, ef.metrics, workerID, w, reader)
}

func (ef *egressWorkerRunnerFactory[WArgs, In, W]) runIO(ctx context.Context) {
	ef.input.run(ctx)
}

func (ef *egressWorkerRunnerFactory[WArgs, In, W]) closeIO() {}
