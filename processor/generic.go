package processor

import (
	"context"
	"fmt"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"github.com/FerroO2000/goccia/internal/telemetry"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default configuration values for the generic processor stage.
const (
	DefaultGenericConfigName = "generic"
)

// GenericConfig structs contains the configuration for a generic processor stage.
type GenericConfig struct {
	*config.Base

	// Name is the name of the stage.
	// It is used to identify the stage in the telemetry.
	Name string
}

// NewGenericConfig returns the default configuration for a generic processor stage.
func NewGenericConfig(runningMode config.StageRunningMode) *GenericConfig {
	return &GenericConfig{
		Base: config.NewBase(runningMode),

		Name: DefaultGenericConfigName,
	}
}

// Validate checks the configuration.
func (c *GenericConfig) Validate(ac *config.AnomalyCollector) {
	c.Base.Validate(ac)

	config.CheckNotEmpty(ac, "Name", &c.Name, DefaultGenericConfigName)
}

// ─── Handler ────────────────────────────────────────────────────────────────|

// GenericHandler interface defines the methods
// a generic processor stage handler must implement.
type GenericHandler[In, Out msgBody] interface {
	// Init method is called once when the stage is initialized.
	Init(ctx context.Context) error

	// Handle method is called by one of the spawned workers
	// for each message received by the stage.
	// It shall return the output message and any error.
	Handle(ctx context.Context, msgIn In) (Out, error)

	// Close is called once when the stage is closed.
	Close()

	// SetTelemetry sets the telemetry for the custom handler.
	// It can be used to add traces, logs, and metrics to the
	// user defined handler.
	SetTelemetry(tel *telemetry.Telemetry)
}

// GenericHandlerBase is a base implementation of the GenericHandler interface.
// It provides a Telemetry field that can be used to add traces,
// logs, and metrics to the generic handler.
// It also provides a default implementation for the Init and Close methods,
// but not for the Handle method.
type GenericHandlerBase struct {
	Telemetry *telemetry.Telemetry
}

// Init is a no-op implementation of the generic handler Init method.
func (chb *GenericHandlerBase) Init(_ context.Context) error {
	return nil
}

// Close is a no-op implementation of the generic handler Close method.
func (chb *GenericHandlerBase) Close() {}

// SetTelemetry sets the telemetry for the generic handler.
func (chb *GenericHandlerBase) SetTelemetry(tel *telemetry.Telemetry) {
	chb.Telemetry = tel
}

// ─── Environment ────────────────────────────────────────────────────────────|

type genericEnv[In, Out msgBody] struct {
	*env.BaseEnv[*GenericConfig, *metrics.EmptyMetrics]

	handler     GenericHandler[In, Out]
	traceString string
}

func newGenericEnv[In, Out msgBody](config *GenericConfig, handler GenericHandler[In, Out]) *genericEnv[In, Out] {
	return &genericEnv[In, Out]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		handler:     handler,
		traceString: "",
	}
}

func (ge *genericEnv[In, Out]) Init(ctx context.Context) error {
	if err := ge.BaseEnv.Init(ctx); err != nil {
		return err
	}

	ge.traceString = fmt.Sprintf("handle %s message", ge.Config.Name)

	ge.handler.SetTelemetry(ge.Tel)
	return ge.handler.Init(ctx)
}

func (ge *genericEnv[In, Out]) Close(ctx context.Context) {
	ge.BaseEnv.Close(ctx)

	ge.handler.Close()
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type genericWorker[In, Out msgBody] struct {
	worker.BaseWorker[*genericEnv[In, Out]]
}

func newGenericWorkerMaker[In, Out msgBody]() func() *genericWorker[In, Out] {
	return func() *genericWorker[In, Out] {
		return &genericWorker[In, Out]{}
	}
}

func (gw *genericWorker[In, Out]) Handle(ctx context.Context, msgIn *msg[In]) (*msg[Out], error) {
	ctx, span := gw.Tel.StartTrace(ctx, gw.Env.traceString)
	defer span.End()

	// Call the provided handler
	msgoutBody, err := gw.Env.handler.Handle(ctx, msgIn.GetBody())
	if err != nil {
		return &msg[Out]{}, err
	}

	msgOut := message.NewMessage(msgoutBody)
	msgOut.SaveSpan(span)

	return msgOut, nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*GenericStage[msgBody, msgBody])(nil)

// GenericStage is a generic egress stage that uses an user defined handler
// for processing messages.
type GenericStage[In, Out msgBody] struct {
	*stage.ProcessorStage[In, Out, *genericEnv[In, Out]]
}

// NewGenericStage returns a new generic processor stage.
func NewGenericStage[In, Out msgBody](
	handler GenericHandler[In, Out], inConnector msgConn[In], outConnector msgConn[Out], cfg *GenericConfig,
) *GenericStage[In, Out] {

	env := newGenericEnv(cfg, handler)

	return &GenericStage[In, Out]{
		ProcessorStage: stage.NewProcessorStage(
			cfg.Name, inConnector, outConnector, env, newGenericWorkerMaker[In, Out](), cfg.Stage,
		),
	}
}
