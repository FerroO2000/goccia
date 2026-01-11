package processor

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/rob"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

// Default configuration values for the re-order buffer stage.
const (
	DefaultROBConfigMaxSeqNum           = 255
	DefaultROBConfigPrimaryBufferSize   = 128
	DefaultROBConfigAuxiliaryBufferSize = 128
	DefaultROBConfigFlushTreshold       = 0.3
	DefaultROBConfigTimeSmootherEnabled = true
	DefaultROBConfigEstimatorAlpha      = 0.8
	DefaultROBConfigEstimatorBeta       = 0.5
	DefaultROBConfigResetTimeout        = 100 * time.Millisecond
)

// ROBConfig structs contains the configuration for the re-order buffer stage.
type ROBConfig struct {
	// MaxSeqNum is the maximum possible sequence number.
	MaxSeqNum uint64

	// PrimaryBufferSize is the size of the primary buffer.
	PrimaryBufferSize uint64

	// AuxiliaryBufferSize is the size of the auxiliary buffer.
	AuxiliaryBufferSize uint64

	// FlushTreshold is the value of the fullness of the auxiliary buffer
	// needed for flushing the primary buffer.
	FlushTreshold float64

	// TimeSmootherEnabled states whether the time smoother is enabled or not.
	TimeSmootherEnabled bool

	// EstimatorAlpha is the value for the alpha parameter for
	// the double exponential estimator (data smoothing factor).
	// It must be between 0 and 1.
	EstimatorAlpha float64

	// EstimatorBeta is the value for the beta parameter for
	// the double exponential estimator (trend smoothing factor).
	// It must be between 0 and 1.
	EstimatorBeta float64

	// ResetTimeout is the timeout for resetting the re-order buffer.
	ResetTimeout time.Duration
}

// NewROBConfig returns the default configuration for the re-order buffer stage.
func NewROBConfig() *ROBConfig {
	return &ROBConfig{
		MaxSeqNum:           DefaultROBConfigMaxSeqNum,
		PrimaryBufferSize:   DefaultROBConfigPrimaryBufferSize,
		AuxiliaryBufferSize: DefaultROBConfigAuxiliaryBufferSize,
		FlushTreshold:       DefaultROBConfigFlushTreshold,
		EstimatorAlpha:      DefaultROBConfigEstimatorAlpha,
		EstimatorBeta:       DefaultROBConfigEstimatorBeta,
		ResetTimeout:        DefaultROBConfigResetTimeout,
	}
}

// Validate checks the configuration.
func (c *ROBConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotZero(ac, "MaxSeqNum", &c.MaxSeqNum, DefaultROBConfigMaxSeqNum)

	config.CheckNotZero(ac, "PrimaryBufferSize", &c.PrimaryBufferSize, DefaultROBConfigPrimaryBufferSize)
	config.CheckNotGreaterThan(ac, "PrimaryBufferSize", "MaxSeqNum", &c.PrimaryBufferSize, c.MaxSeqNum+1)

	config.CheckNotZero(ac, "AuxiliaryBufferSize", &c.AuxiliaryBufferSize, DefaultROBConfigAuxiliaryBufferSize)
	config.CheckNotGreaterThan(ac,
		"AuxiliaryBufferSize", "MaxSeqNum - PrimaryBufferSize",
		&c.AuxiliaryBufferSize, c.MaxSeqNum+1-c.PrimaryBufferSize,
	)

	config.CheckNotNegative(ac, "FlushTreshold", &c.FlushTreshold, DefaultROBConfigFlushTreshold)
	config.CheckNotZero(ac, "FlushTreshold", &c.FlushTreshold, DefaultROBConfigFlushTreshold)

	config.CheckNotNegative(ac, "EstimatorAlpha", &c.EstimatorAlpha, DefaultROBConfigEstimatorAlpha)
	config.CheckNotZero(ac, "EstimatorAlpha", &c.EstimatorAlpha, DefaultROBConfigEstimatorAlpha)
	config.CheckNotGreaterThan(ac, "EstimatorAlpha", "1", &c.EstimatorAlpha, 1.0)

	config.CheckNotNegative(ac, "EstimatorBeta", &c.EstimatorBeta, DefaultROBConfigEstimatorBeta)
	config.CheckNotZero(ac, "EstimatorBeta", &c.EstimatorBeta, DefaultROBConfigEstimatorBeta)
	config.CheckNotGreaterThan(ac, "EstimatorBeta", "1", &c.EstimatorBeta, 1.0)

	config.CheckNotNegative(ac, "ResetTimeout", &c.ResetTimeout, DefaultROBConfigResetTimeout)
}

/////////////
//  STAGE  //
/////////////

// ROBStage is the re-order buffer stage.
// It can only be run in single-threaded mode.
type ROBStage[T message.ReOrderable] struct {
	tel *internal.Telemetry

	cfg *ROBConfig

	inputConnector  msgConn[T]
	outputConnector msgConn[T]

	rob *rob.ROB[*msg[T]]

	// Metrics
	orderedMsgs           atomic.Int64
	primayEnqueuedMsgs    atomic.Int64
	auxiliaryEnqueuedMsgs atomic.Int64

	outOfOrderSeqNum atomic.Int64
	duplicatedSeqNum atomic.Int64
	invalidSeqNum    atomic.Int64

	resets atomic.Int64
}

// NewROBStage returns a new re-order buffer stage.
func NewROBStage[T message.ReOrderable](inConnector, outConnector msgConn[T], cfg *ROBConfig) *ROBStage[T] {
	tel := internal.NewTelemetry("processor", "rob")

	return &ROBStage[T]{
		tel: tel,

		cfg: cfg,

		inputConnector:  inConnector,
		outputConnector: outConnector,
	}
}

// Init initializes the stage.
func (rs *ROBStage[T]) Init(_ context.Context) error {
	rs.tel.LogInfo("initializing")

	rs.rob = rob.NewROB(rs.outputConnector, &rob.Config{
		MaxSeqNum:           rs.cfg.MaxSeqNum,
		PrimaryBufferSize:   rs.cfg.PrimaryBufferSize,
		AuxiliaryBufferSize: rs.cfg.AuxiliaryBufferSize,
		FlushTreshold:       rs.cfg.FlushTreshold,
		TimeSmootherEnabled: rs.cfg.TimeSmootherEnabled,
		EstimatorAlpha:      rs.cfg.EstimatorAlpha,
		EstimatorBeta:       rs.cfg.EstimatorBeta,
	})

	rs.initMetrics()

	return nil
}

func (rs *ROBStage[T]) initMetrics() {
	rs.tel.NewCounter("ordered_messages", func() int64 { return rs.orderedMsgs.Load() })
	rs.tel.NewCounter("primary_enqueued_messages", func() int64 { return rs.primayEnqueuedMsgs.Load() })
	rs.tel.NewCounter("auxiliary_enqueued_messages", func() int64 { return rs.auxiliaryEnqueuedMsgs.Load() })

	rs.tel.NewCounter("out_of_order_sequence_number", func() int64 { return rs.outOfOrderSeqNum.Load() })
	rs.tel.NewCounter("duplicated_sequence_number", func() int64 { return rs.duplicatedSeqNum.Load() })
	rs.tel.NewCounter("invalid_sequence_number", func() int64 { return rs.invalidSeqNum.Load() })

	rs.tel.NewCounter("resets", func() int64 { return rs.resets.Load() })
}

// Run runs the re-order buffer stage.
func (rs *ROBStage[T]) Run(ctx context.Context) {
	rs.tel.LogInfo("running")

	resetNeeded := false
	for {
		select {
		case <-ctx.Done():
			// Context is done, flush the ROB and return
			rs.rob.FlushAndReset()
			return

		default:
		}

		// Read the next message with a timeout context
		// in order to reset the re-order buffer
		deadlineCtx, cancelCtx := context.WithTimeout(ctx, rs.cfg.ResetTimeout)
		msgIn, err := rs.inputConnector.Read(deadlineCtx)
		cancelCtx()

		if err != nil {
			if errors.Is(err, connector.ErrClosed) {
				return
			}

			// This means the context is done.
			// Check if the rob has to be reset
			if resetNeeded {
				rs.rob.FlushAndReset()
				rs.resets.Add(1)
				resetNeeded = false

				rs.tel.LogInfo("resetting and flushing re-order buffer")
			}

			continue
		}

		// Set the sequence number encoded in the message
		// value into the main message struct
		msgIn.SetSequenceNumber(msgIn.GetBody().GetSequenceNumber())

		// Try to enqueue the message
		rs.enqueue(ctx, msgIn)

		resetNeeded = true
	}
}

func (rs *ROBStage[T]) enqueue(ctx context.Context, msgIn *msg[T]) {
	_, span := rs.tel.NewTrace(msgIn.LoadSpanContext(ctx), "enqueue message into re-order buffer")
	defer span.End()

	status, err := rs.rob.Enqueue(msgIn)
	if err != nil {
		if errors.Is(err, rob.ErrSeqNumOutOfWindow) {
			rs.outOfOrderSeqNum.Add(1)
		} else if errors.Is(err, rob.ErrSeqNumDuplicated) {
			rs.duplicatedSeqNum.Add(1)
		} else if errors.Is(err, rob.ErrSeqNumTooBig) {
			rs.invalidSeqNum.Add(1)
		}
	}

	span.SetAttributes(attribute.String("status", status.String()))

	switch status {
	case rob.EnqueueStatusInOrder:
		rs.orderedMsgs.Add(1)
	case rob.EnqueueStatusPrimary:
		rs.primayEnqueuedMsgs.Add(1)
	case rob.EnqueueStatusAuxiliary:
		rs.auxiliaryEnqueuedMsgs.Add(1)
	case rob.EnqueueStatusErr:
		return
	}
}

// Close closes the stage.
func (rs *ROBStage[T]) Close() {
	rs.tel.LogInfo("closing")
	defer rs.tel.LogInfo("closed")

	rs.outputConnector.Close()
}
