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
	tel *telemetry.Telemetry

	config Cfg

	processorMetrics *stageMetrics.ProcessorStage
	egressMetrics    *stageMetrics.EgressStage

	Metrics M
}

func newBaseEnv[Cfg cfg, M met](config Cfg, metrics M) *BaseEnv[Cfg, M] {
	return &BaseEnv[Cfg, M]{
		tel: nil,

		config: config,

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
	e.tel = tel
}

func (e *BaseEnv[Cfg, M]) Telemetry() *telemetry.Telemetry {
	return e.tel
}

func (e *BaseEnv[Cfg, M]) validateConfig() {
	e.tel.LogDebug("validating configuration")
	defer e.tel.LogDebug("validated configuration")

	configValidator := config.NewValidator(e.tel)
	configValidator.Validate(e.config)
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
	e.tel.LogDebug("initializing metrics")
	defer e.tel.LogDebug("initialized metrics")

	if err := e.getStageMetrics().InitMetrics(e.tel); err != nil {
		return err
	}

	return e.Metrics.InitMetrics(e.tel)
}

func (e *BaseEnv[Cfg, M]) Init(_ context.Context) error {
	e.tel.LogDebug("initializing environment")
	defer e.tel.LogDebug("initialized environment")

	e.validateConfig()
	return e.initMetrics()
}

func (e *BaseEnv[Cfg, M]) Close(_ context.Context) {
	e.tel.LogDebug("closing environment")
	defer e.tel.LogDebug("closed environment")
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
