package egress

import (
	"context"
	"fmt"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default configuration values for the generic egress stage.
const (
	DefaultGenericConfigName = "generic"
)

// GenericConfig structs contains the configuration for a generic egress stage.
type GenericConfig struct {
	*config.Base

	// Name sets the name of the stage.
	// It is used to identify the stage in the telemetry data.
	Name string
}

// NewGenericConfig returns the default configuration for a generic egress stage.
func NewGenericConfig(name string, runningMode config.StageRunningMode) *GenericConfig {
	return &GenericConfig{
		Base: config.NewBase(runningMode),

		Name: name,
	}
}

// Validate checks the configuration.
func (c *GenericConfig) Validate(ac *config.AnomalyCollector) {
	c.Base.Validate(ac)

	config.CheckNotEmpty(ac, "Name", &c.Name, DefaultGenericConfigName)
}

// ─── Handler ────────────────────────────────────────────────────────────────|

// GenericHandler interface defines the methods
// a generic egress stage handler must implement.
type GenericHandler[In msgBody] interface {
	// Init method is called once when the stage is initialized.
	Init(ctx context.Context) error

	// Handle method is called by one of the spawned workers
	// for each message received by the stage.
	Handle(ctx context.Context, msgIn In) error

	// Close is called once when the stage is closed.
	Close()

	// SetTelemetry sets the telemetry for the custom handler.
	// It can be used to add traces, logs, and metrics to the
	// user defined handler.
	SetTelemetry(tel *telemetry.Telemetry)
}

// GenericHandlerBase is a base implementation of the GenericHandler interface.
// It provides a Telemetry field that can be used to add traces,
// logs, and metrics to the custom handler.
// It also provides a default implementation for the Init and Close methods,
// but not for the Handle method.
type GenericHandlerBase struct {
	Telemetry *telemetry.Telemetry
}

// Init is a no-op implementation of the generic handler Init method.
func (ghb *GenericHandlerBase) Init(_ context.Context) error {
	return nil
}

// Close is a no-op implementation of the generic handler Close method.
func (ghb *GenericHandlerBase) Close() {}

// SetTelemetry sets the telemetry for the generic handler.
func (ghb *GenericHandlerBase) SetTelemetry(tel *telemetry.Telemetry) {
	ghb.Telemetry = tel
}

// ─── Environment ────────────────────────────────────────────────────────────|

type genericEnv[In msgBody] struct {
	*env.BaseEnv[*GenericConfig, *metrics.EmptyMetrics]

	handler     GenericHandler[In]
	traceString string
}

func newGenericEnv[In msgBody](config *GenericConfig, handler GenericHandler[In]) *genericEnv[In] {
	return &genericEnv[In]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		handler:     handler,
		traceString: "",
	}
}

func (ge *genericEnv[In]) Init(ctx context.Context) error {
	if err := ge.BaseEnv.Init(ctx); err != nil {
		return err
	}

	ge.traceString = fmt.Sprintf("handle %s message", ge.Config.Name)

	ge.handler.SetTelemetry(ge.Tel)
	return ge.handler.Init(ctx)
}

func (ge *genericEnv[In]) Close(ctx context.Context) {
	ge.BaseEnv.Close(ctx)

	ge.handler.Close()
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type genericWorker[In msgBody] struct {
	worker.BaseWorker[*genericEnv[In]]
}

func newGenericWorkerMaker[In msgBody]() func() *genericWorker[In] {
	return func() *genericWorker[In] {
		return &genericWorker[In]{}
	}
}

func (gw *genericWorker[In]) Deliver(ctx context.Context, msgIn *msg[In]) error {
	ctx, span := gw.Tel.StartTrace(ctx, gw.Env.traceString)
	defer span.End()

	// Call the provided handler
	return gw.Env.handler.Handle(ctx, msgIn.GetBody())
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// GenericStage is a generic egress stage that uses an user defined handler
// for delivering messages.
type GenericStage[In msgBody] struct {
	*stage.EgressStage[In, *genericEnv[In]]
}

// NewGenericStage returns a new generic egress stage.
func NewGenericStage[In msgBody](
	handler GenericHandler[In], inConnector msgConn[In], cfg *GenericConfig,
) *GenericStage[In] {

	env := newGenericEnv(cfg, handler)

	return &GenericStage[In]{
		EgressStage: stage.NewEgressStage(
			cfg.Name, inConnector, env, newGenericWorkerMaker[In](), cfg.Stage,
		),
	}
}
