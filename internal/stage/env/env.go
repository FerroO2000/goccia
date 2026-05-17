package env

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/metrics"
	stageMetrics "github.com/FerroO2000/goccia/internal/stage/metrics"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

type cfg = config.Config
type met = metrics.Metrics

type Env interface {
	SetTelemetry(tel *telemetry.Telemetry)
	Telemetry() *telemetry.Telemetry
	Init(ctx context.Context) error
	Close(ctx context.Context)

	GetProcessorMetrics() *stageMetrics.ProcessorStage
	GetEgressMetrics() *stageMetrics.EgressStage
}

var _ Env = (*BaseEnv[cfg, met])(nil)

type BaseEnv[Cfg cfg, M met] struct {
	Tel *telemetry.Telemetry

	Config Cfg

	processorMetrics *stageMetrics.ProcessorStage
	egressMetrics    *stageMetrics.EgressStage

	Metrics M
}

func newBaseEnv[Cfg cfg, M met](config Cfg, metrics M) *BaseEnv[Cfg, M] {
	return &BaseEnv[Cfg, M]{
		Tel: nil,

		Config: config,

		processorMetrics: nil,
		egressMetrics:    nil,

		Metrics: metrics,
	}
}

func NewProcessorEnv[Cfg cfg, M met](config Cfg, metrics M) *BaseEnv[Cfg, M] {
	env := newBaseEnv(config, metrics)
	env.processorMetrics = stageMetrics.NewProcessorStage()
	return env
}

func NewEgressEnv[Cfg cfg, M met](config Cfg, metrics M) *BaseEnv[Cfg, M] {
	env := newBaseEnv(config, metrics)
	env.egressMetrics = stageMetrics.NewEgressStage()
	return env
}

func (e *BaseEnv[Cfg, M]) SetTelemetry(tel *telemetry.Telemetry) {
	e.Tel = tel
}

func (e *BaseEnv[Cfg, M]) Telemetry() *telemetry.Telemetry {
	return e.Tel
}

func (e *BaseEnv[Cfg, M]) validateConfig() {
	e.Tel.LogDebug("validating configuration")
	defer e.Tel.LogDebug("validated configuration")

	configValidator := config.NewValidator(e.Tel)
	configValidator.Validate(e.Config)
}

func (e *BaseEnv[Cfg, M]) getStageMetrics() met {
	if e.processorMetrics != nil {
		return e.processorMetrics
	}

	if e.egressMetrics != nil {
		return e.egressMetrics
	}

	panic("missing stage metrics")
}

func (e *BaseEnv[Cfg, M]) initMetrics() error {
	e.Tel.LogDebug("initializing metrics")
	defer e.Tel.LogDebug("initialized metrics")

	if err := e.getStageMetrics().InitMetrics(e.Tel); err != nil {
		return err
	}

	return e.Metrics.InitMetrics(e.Tel)
}

func (e *BaseEnv[Cfg, M]) Init(_ context.Context) error {
	e.Tel.LogDebug("initializing environment")
	defer e.Tel.LogDebug("initialized environment")

	e.validateConfig()
	return e.initMetrics()
}

func (e *BaseEnv[Cfg, M]) Close(_ context.Context) {
	e.Tel.LogDebug("closing environment")
	defer e.Tel.LogDebug("closed environment")
}

func (e *BaseEnv[Cfg, M]) GetProcessorMetrics() *stageMetrics.ProcessorStage {
	if e.processorMetrics == nil {
		panic("no processor metrics")
	}

	return e.processorMetrics
}

func (e *BaseEnv[Cfg, M]) GetEgressMetrics() *stageMetrics.EgressStage {
	if e.egressMetrics == nil {
		panic("no egress metrics")
	}

	return e.egressMetrics
}
