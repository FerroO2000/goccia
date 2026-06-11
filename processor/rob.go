package processor

import (
	"context"
	"errors"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/rob"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/processor/metrics"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default configuration values for the re-order buffer stage.
const (
	DefaultROBConfigMaxSeqNum           = rob.DefaultMaxSeqNum
	DefaultROBConfigPrimaryBufferSize   = rob.DefaultPrimaryBufferSize
	DefaultROBConfigAuxiliaryBufferSize = rob.DefaultAuxiliaryBufferSize
	DefaultROBConfigFlushTreshold       = rob.DefaultFlushTreshold
	DefaultROBConfigTimeSmootherEnabled = rob.DefaultTimeSmootherEnabled
	DefaultROBConfigEstimatorAlpha      = rob.DefaultEstimatorAlpha
	DefaultROBConfigEstimatorBeta       = rob.DefaultEstimatorBeta
	DefaultROBConfigResetTimeout        = 100 * time.Millisecond
)

type robConfig = rob.Config

// ROBConfig structs contains the configuration for the re-order buffer stage.
type ROBConfig struct {
	*robConfig

	// ResetTimeout is the timeout for resetting the re-order buffer.
	ResetTimeout time.Duration
}

// NewROBConfig returns the default configuration for the re-order buffer stage.
func NewROBConfig() *ROBConfig {
	return &ROBConfig{
		robConfig: rob.NewConfig(),

		ResetTimeout: DefaultROBConfigResetTimeout,
	}
}

// Validate checks the configuration.
func (c *ROBConfig) Validate(ac *config.AnomalyCollector) {
	c.robConfig.Validate(ac)

	config.CheckNotNegative(ac, "ResetTimeout", &c.ResetTimeout, DefaultROBConfigResetTimeout)
}

// ─── Environment ────────────────────────────────────────────────────────────|

type robEnv struct {
	*env.BaseEnv[*ROBConfig, *metrics.RobStage]

	resetTimeout time.Duration
}

func newROBEnv(config *ROBConfig) *robEnv {
	return &robEnv{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewRobStage()),

		resetTimeout: config.ResetTimeout,
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*robEnv] = (*robRunner[message.ReOrderable])(nil)

type robRunner[T message.ReOrderable] struct {
	*robEnv

	inConnector  msgConn[T]
	outConnector msgConn[T]

	runDone chan struct{}

	rob *rob.ROB[*msg[T]]
}

func newROBRunner[T message.ReOrderable](inConnector, outConnector msgConn[T]) *robRunner[T] {
	return &robRunner[T]{
		inConnector:  inConnector,
		outConnector: outConnector,

		runDone: make(chan struct{}),
	}
}

func (rr *robRunner[T]) SetEnvironment(env *robEnv) {
	rr.robEnv = env
}

func (rr *robRunner[T]) Init(_ context.Context) error {
	robCfg := &rob.Config{
		MaxSeqNum:           rr.Config.MaxSeqNum,
		PrimaryBufferSize:   rr.Config.PrimaryBufferSize,
		AuxiliaryBufferSize: rr.Config.AuxiliaryBufferSize,
		FlushTreshold:       rr.Config.FlushTreshold,
		TimeSmootherEnabled: rr.Config.TimeSmootherEnabled,
		EstimatorAlpha:      rr.Config.EstimatorAlpha,
		EstimatorBeta:       rr.Config.EstimatorBeta,
	}
	rr.rob = rob.NewROB(rr.outConnector, robCfg)

	return nil
}

func (rr *robRunner[T]) Run(ctx context.Context) {
	defer close(rr.runDone)

	resetNeeded := false

	for {
		// Read the next message with a timeout context
		// in order to reset the re-order buffer
		deadlineCtx, cancelCtx := context.WithTimeout(ctx, rr.resetTimeout)
		msgIn, err := rr.inConnector.Read(deadlineCtx)
		cancelCtx()

		if err != nil {
			if errors.Is(err, connector.ErrClosed) || ctx.Err() != nil {
				// Drain buffered connector messages before flushing the ROB.
				rr.rob.FlushAndReset()
				return
			}

			// The read deadline expired. Check if the ROB has to be reset.
			if resetNeeded {
				rr.rob.FlushAndReset()
				rr.Metrics.IncrementResets()
				resetNeeded = false

				rr.Telemetry().LogInfo("resetting and flushing re-order buffer")
			}

			continue
		}

		// Set the sequence number encoded in the message
		// value into the main message struct
		msgIn.SetSequenceNumber(msgIn.GetBody().GetSequenceNumber())

		// Try to enqueue the message
		rr.enqueue(ctx, msgIn)

		resetNeeded = true
	}
}

func (rr *robRunner[T]) enqueue(ctx context.Context, msgIn *msg[T]) {
	rr.Tel.LogDebug("enqueued message into re-order buffer")

	_, span := rr.Telemetry().StartTrace(msgIn.LoadSpanContext(ctx), "enqueue message into re-order buffer")
	defer span.End()

	status, err := rr.rob.Enqueue(msgIn)
	if err != nil {
		if errors.Is(err, rob.ErrSeqNumOutOfWindow) {
			rr.Metrics.IncrementOutOfOrderSequenceNumber()
		} else if errors.Is(err, rob.ErrSeqNumDuplicated) {
			rr.Metrics.IncrementDuplicatedSequenceNumber()
		} else if errors.Is(err, rob.ErrSeqNumTooBig) {
			rr.Metrics.IncrementInvalidSequenceNumber()
		}
	}

	span.SetAttributes(attribute.String("status", status.String()))

	switch status {
	case rob.EnqueueStatusInOrder:
		rr.Metrics.IncrementOrderedMessages()
	case rob.EnqueueStatusPrimary:
		rr.Metrics.IncrementPrimaryEnqueuedMessages()
	case rob.EnqueueStatusAuxiliary:
		rr.Metrics.IncrementAuxiliaryEnqueuedMessages()
	case rob.EnqueueStatusErr:
		return
	}
}

func (rr *robRunner[T]) Close(_ context.Context) {
	<-rr.runDone
	rr.outConnector.Close()
}

func (rr *robRunner[T]) Inputs() []uintptr {
	return []uintptr{connector.GetConnectorID(rr.inConnector)}
}

func (rr *robRunner[T]) Outputs() []uintptr {
	return []uintptr{connector.GetConnectorID(rr.outConnector)}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*ROBStage[message.ReOrderable])(nil)

// ROBStage is the re-order buffer stage.
// It can only be run in single-threaded mode.
type ROBStage[T message.ReOrderable] struct {
	*stage.ProcessorStage[T, T, *robEnv]
}

// NewROBStage returns a new re-order buffer stage.
func NewROBStage[T message.ReOrderable](
	inConnector, outConnector msgConn[T], cfg *ROBConfig,
) *ROBStage[T] {

	return &ROBStage[T]{
		ProcessorStage: stage.NewProcessorStageFromRunner[T, T](
			"rob", newROBEnv(cfg), newROBRunner(inConnector, outConnector),
		),
	}
}
