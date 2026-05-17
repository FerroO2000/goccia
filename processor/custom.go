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

// Default configuration values for the custom processor stage.
const (
	DefaultCustomConfigName = "custom"
)

// CustomConfig structs contains the configuration for a custom processor stage.
type CustomConfig struct {
	*config.Base

	// Name is the name of the stage.
	// It is used to identify the stage in the telemetry.
	Name string
}

// NewCustomConfig returns the default configuration for a custom processor stage.
func NewCustomConfig(runningMode config.StageRunningMode) *CustomConfig {
	return &CustomConfig{
		Base: config.NewBase(runningMode),

		Name: DefaultCustomConfigName,
	}
}

// Validate checks the configuration.
func (c *CustomConfig) Validate(ac *config.AnomalyCollector) {
	c.Base.Validate(ac)

	config.CheckNotEmpty(ac, "Name", &c.Name, DefaultCustomConfigName)
}

// ─── Handler ────────────────────────────────────────────────────────────────|

// CustomHandler interface defines the methods that the handler for the
// processor processor must implement.
type CustomHandler[In, Out msgBody] interface {
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

// CustomHandlerBase is a base implementation of the CustomHandler interface.
// It provides a Telemetry field that can be used to add traces,
// logs, and metrics to the custom handler.
// It also provides a default implementation for the Init and Close methods,
// but not for the Handle method.
type CustomHandlerBase struct {
	Telemetry *telemetry.Telemetry
}

// Init is a no-op implementation of the custom handler Init method.
func (chb *CustomHandlerBase) Init(_ context.Context) error {
	return nil
}

// Close is a no-op implementation of the custom handler Close method.
func (chb *CustomHandlerBase) Close() {}

// SetTelemetry sets the telemetry for the custom handler.
func (chb *CustomHandlerBase) SetTelemetry(tel *telemetry.Telemetry) {
	chb.Telemetry = tel
}

// ─── Environment ────────────────────────────────────────────────────────────|

type customEnv[In, Out msgBody] struct {
	*env.BaseEnv[*CustomConfig, *metrics.EmptyMetrics]

	handler     CustomHandler[In, Out]
	traceString string
}

func newCustomEnv[In, Out msgBody](config *CustomConfig, handler CustomHandler[In, Out]) *customEnv[In, Out] {
	return &customEnv[In, Out]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		handler:     handler,
		traceString: fmt.Sprintf("handle %s message", config.Name),
	}
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type customWorker[In, Out msgBody] struct {
	worker.BaseWorker[*customEnv[In, Out]]
}

func newCustomWorkerMaker[In, Out msgBody]() func() *customWorker[In, Out] {
	return func() *customWorker[In, Out] {
		return &customWorker[In, Out]{}
	}
}

func (cw *customWorker[In, Out]) Handle(ctx context.Context, msgIn *msg[In]) (*msg[Out], error) {
	ctx, span := cw.Tel.StartTrace(ctx, cw.Env.traceString)
	defer span.End()

	// Call the provided handler
	msgoutBody, err := cw.Env.handler.Handle(ctx, msgIn.GetBody())
	if err != nil {
		return &msg[Out]{}, err
	}

	msgOut := message.NewMessage(msgoutBody)
	msgOut.SaveSpan(span)

	return msgOut, nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*CustomStage[msgBody, msgBody])(nil)

// CustomStage is a processor stage that uses a custom handler to process messages.
type CustomStage[In, Out msgBody] struct {
	*stage.ProcessorStage[In, Out, *customEnv[In, Out]]
}

// NewCustomStage returns a new custom processor stage.
func NewCustomStage[In, Out msgBody](
	handler CustomHandler[In, Out], inputConnector msgConn[In], outputConnector msgConn[Out], cfg *CustomConfig,
) *CustomStage[In, Out] {

	env := newCustomEnv(cfg, handler)

	return &CustomStage[In, Out]{
		ProcessorStage: stage.NewProcessorStage(
			cfg.Name, inputConnector, outputConnector, env, newCustomWorkerMaker[In, Out](), cfg.Stage,
		),
	}
}

// Init initializes the stage and the custom handler.
func (cs *CustomStage[In, Out]) Init(ctx context.Context) error {
	// Initialize the handler
	handler := cs.Env().handler

	handler.SetTelemetry(cs.Telemetry())
	if err := handler.Init(ctx); err != nil {
		return err
	}

	return cs.Init(ctx)
}

// Close closes the stage and the custom handler.
func (cs *CustomStage[In, Out]) Close(ctx context.Context) {
	// Close the custom handler
	cs.Env().handler.Close()

	cs.Close(ctx)
}
