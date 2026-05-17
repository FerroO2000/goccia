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

type robEnv[T message.ReOrderable] struct {
	*env.BaseEnv[*ROBConfig, *metrics.RobStage]

	inConnector  msgConn[T]
	outConnector msgConn[T]

	rob *rob.ROB[*msg[T]]

	resetTimeout time.Duration
}

func newROBEnv[T message.ReOrderable](config *ROBConfig, inConnector, outConnector msgConn[T]) *robEnv[T] {
	robCfg := &rob.Config{
		MaxSeqNum:           config.MaxSeqNum,
		PrimaryBufferSize:   config.PrimaryBufferSize,
		AuxiliaryBufferSize: config.AuxiliaryBufferSize,
		FlushTreshold:       config.FlushTreshold,
		TimeSmootherEnabled: config.TimeSmootherEnabled,
		EstimatorAlpha:      config.EstimatorAlpha,
		EstimatorBeta:       config.EstimatorBeta,
	}
	rob := rob.NewROB(outConnector, robCfg)

	return &robEnv[T]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewRobStage()),

		inConnector:  inConnector,
		outConnector: outConnector,

		rob: rob,

		resetTimeout: config.ResetTimeout,
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*robEnv[message.ReOrderable]] = (*robRunner[message.ReOrderable])(nil)

type robRunner[T message.ReOrderable] struct {
	*robEnv[T]

	runDone chan struct{}
}

func newROBRunner[T message.ReOrderable]() *robRunner[T] {
	return &robRunner[T]{
		runDone: make(chan struct{}),
	}
}

func (rr *robRunner[T]) SetEnvironment(env *robEnv[T]) {
	rr.robEnv = env
}

func (rr *robRunner[T]) Init(_ context.Context) error {
	return nil
}

func (rr *robRunner[T]) Run(ctx context.Context) {
	rr.Telemetry().LogInfo("running")
	defer rr.Telemetry().LogInfo("stopped")

	resetNeeded := false
	for {
		select {
		case <-ctx.Done():
			// Context is done, flush the ROB and return
			rr.rob.FlushAndReset()
			return

		default:
		}

		// Read the next message with a timeout context
		// in order to reset the re-order buffer
		deadlineCtx, cancelCtx := context.WithTimeout(ctx, rr.resetTimeout)
		msgIn, err := rr.inConnector.Read(deadlineCtx)
		cancelCtx()

		if err != nil {
			if errors.Is(err, connector.ErrClosed) {
				return
			}

			// This means the context is done.
			// Check if the rob has to be reset
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
	*stage.ProcessorStage[T, T, *robEnv[T]]
}

// NewROBStage returns a new re-order buffer stage.
func NewROBStage[T message.ReOrderable](
	inConnector, outConnector msgConn[T], cfg *ROBConfig,
) *ROBStage[T] {

	env := newROBEnv(cfg, inConnector, outConnector)

	return &ROBStage[T]{
		ProcessorStage: stage.NewProcessorStageFromRunner[T, T]("rob", env, newROBRunner[T]()),
	}
}

// // ROBStage is the re-order buffer stage.
// // It can only be run in single-threaded mode.
// type ROBStage[T message.ReOrderable] struct {
// 	tel *telemetry.Telemetry

// 	cfg *ROBConfig

// 	inputConnector  msgConn[T]
// 	outputConnector msgConn[T]

// 	rob *rob.ROB[*msg[T]]

// 	// Metrics
// 	orderedMsgs           atomic.Int64
// 	primayEnqueuedMsgs    atomic.Int64
// 	auxiliaryEnqueuedMsgs atomic.Int64

// 	outOfOrderSeqNum atomic.Int64
// 	duplicatedSeqNum atomic.Int64
// 	invalidSeqNum    atomic.Int64

// 	resets atomic.Int64
// }

// // NewROBStage returns a new re-order buffer stage.
// func NewROBStage[T message.ReOrderable](inConnector, outConnector msgConn[T], cfg *ROBConfig) *ROBStage[T] {
// 	tel := telemetry.NewTelemetry("processor", "rob")

// 	return &ROBStage[T]{
// 		tel: tel,

// 		cfg: cfg,

// 		inputConnector:  inConnector,
// 		outputConnector: outConnector,
// 	}
// }

// // Init initializes the stage.
// func (rs *ROBStage[T]) Init(_ context.Context) error {
// 	rs.tel.LogInfo("initializing")

// 	rs.rob = rob.NewROB(rs.outputConnector, &rob.Config{
// 		MaxSeqNum:           rs.cfg.MaxSeqNum,
// 		PrimaryBufferSize:   rs.cfg.PrimaryBufferSize,
// 		AuxiliaryBufferSize: rs.cfg.AuxiliaryBufferSize,
// 		FlushTreshold:       rs.cfg.FlushTreshold,
// 		TimeSmootherEnabled: rs.cfg.TimeSmootherEnabled,
// 		EstimatorAlpha:      rs.cfg.EstimatorAlpha,
// 		EstimatorBeta:       rs.cfg.EstimatorBeta,
// 	})

// 	rs.initMetrics()

// 	return nil
// }

// func (rs *ROBStage[T]) initMetrics() {
// 	rs.tel.NewCounterMetric("ordered_messages", func() int64 { return rs.orderedMsgs.Load() })
// 	rs.tel.NewCounterMetric("primary_enqueued_messages", func() int64 { return rs.primayEnqueuedMsgs.Load() })
// 	rs.tel.NewCounterMetric("auxiliary_enqueued_messages", func() int64 { return rs.auxiliaryEnqueuedMsgs.Load() })

// 	rs.tel.NewCounterMetric("out_of_order_sequence_number", func() int64 { return rs.outOfOrderSeqNum.Load() })
// 	rs.tel.NewCounterMetric("duplicated_sequence_number", func() int64 { return rs.duplicatedSeqNum.Load() })
// 	rs.tel.NewCounterMetric("invalid_sequence_number", func() int64 { return rs.invalidSeqNum.Load() })

// 	rs.tel.NewCounterMetric("resets", func() int64 { return rs.resets.Load() })
// }

// // Run runs the re-order buffer stage.
// func (rs *ROBStage[T]) Run(ctx context.Context) {
// 	defer rs.tel.LogInfo("stopped")
// 	rs.tel.LogInfo("running")

// 	resetNeeded := false
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			// Context is done, flush the ROB and return
// 			rs.rob.FlushAndReset()
// 			return

// 		default:
// 		}

// 		// Read the next message with a timeout context
// 		// in order to reset the re-order buffer
// 		deadlineCtx, cancelCtx := context.WithTimeout(ctx, rs.cfg.ResetTimeout)
// 		msgIn, err := rs.inputConnector.Read(deadlineCtx)
// 		cancelCtx()

// 		if err != nil {
// 			if errors.Is(err, connector.ErrClosed) {
// 				return
// 			}

// 			// This means the context is done.
// 			// Check if the rob has to be reset
// 			if resetNeeded {
// 				rs.rob.FlushAndReset()
// 				rs.resets.Add(1)
// 				resetNeeded = false

// 				rs.tel.LogInfo("resetting and flushing re-order buffer")
// 			}

// 			continue
// 		}

// 		// Set the sequence number encoded in the message
// 		// value into the main message struct
// 		msgIn.SetSequenceNumber(msgIn.GetBody().GetSequenceNumber())

// 		// Try to enqueue the message
// 		rs.enqueue(ctx, msgIn)

// 		resetNeeded = true
// 	}
// }

// func (rs *ROBStage[T]) enqueue(ctx context.Context, msgIn *msg[T]) {
// 	_, span := rs.tel.StartTrace(msgIn.LoadSpanContext(ctx), "enqueue message into re-order buffer")
// 	defer span.End()

// 	status, err := rs.rob.Enqueue(msgIn)
// 	if err != nil {
// 		if errors.Is(err, rob.ErrSeqNumOutOfWindow) {
// 			rs.outOfOrderSeqNum.Add(1)
// 		} else if errors.Is(err, rob.ErrSeqNumDuplicated) {
// 			rs.duplicatedSeqNum.Add(1)
// 		} else if errors.Is(err, rob.ErrSeqNumTooBig) {
// 			rs.invalidSeqNum.Add(1)
// 		}
// 	}

// 	span.SetAttributes(attribute.String("status", status.String()))

// 	switch status {
// 	case rob.EnqueueStatusInOrder:
// 		rs.orderedMsgs.Add(1)
// 	case rob.EnqueueStatusPrimary:
// 		rs.primayEnqueuedMsgs.Add(1)
// 	case rob.EnqueueStatusAuxiliary:
// 		rs.auxiliaryEnqueuedMsgs.Add(1)
// 	case rob.EnqueueStatusErr:
// 		return
// 	}
// }

// // Close closes the stage.
// func (rs *ROBStage[T]) Close(_ context.Context) {
// 	rs.tel.LogInfo("closing")
// 	defer rs.tel.LogInfo("closed")

// 	rs.outputConnector.Close()
// }

// func (rs *ROBStage[T]) Name() string {
// 	return "rob"
// }

// func (rs *ROBStage[T]) Kind() stage.Kind {
// 	return stage.KindProcessor
// }

// func (rs *ROBStage[T]) Telemetry() *telemetry.Telemetry {
// 	return rs.tel
// }

// func (rs *ROBStage[T]) Inputs() []uintptr {
// 	return []uintptr{connector.GetConnectorID(rs.inputConnector)}
// }

// func (rs *ROBStage[T]) Outputs() []uintptr {
// 	return []uintptr{connector.GetConnectorID(rs.outputConnector)}
// }
