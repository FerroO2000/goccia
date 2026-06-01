package processor

import (
	"context"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"github.com/FerroO2000/goccia/processor/metrics"
	"github.com/squadracorsepolito/acmelib"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// CANConfig structs contains the configuration for the CAN processor stage.
type CANConfig struct {
	*config.Base

	Messages []*acmelib.Message
}

// NewCANConfig returns the default configuration for the CAN processor stage.
func NewCANConfig(runningMode config.StageRunningMode) *CANConfig {
	return &CANConfig{
		Base: config.NewBase(runningMode),

		Messages: []*acmelib.Message{},
	}
}

// ─── Message ────────────────────────────────────────────────────────────────|

// CANMessageCarrier interface defines the common methods
// for all message types that carry CAN messages.
type CANMessageCarrier interface {
	msgBody

	// GetRawMessages returns the list of raw CAN messages.
	GetRawMessages() []CANRawMessage
}

// CANRawMessage represents a CAN message before decoding.
type CANRawMessage struct {
	// CANID is the CAN ID of the message.
	CANID uint32

	// RawData is the payload of the CAN message.
	RawData []byte
	// DataLen is the number of bytes of the payload.
	DataLen int
}

// CANSignalValueType represents the type of the value of a signal.
type CANSignalValueType int

const (
	// CANSignalValueTypeFlag defines a value of type flag (boolean).
	CANSignalValueTypeFlag CANSignalValueType = iota
	// CANSignalValueTypeInt defines a value of type integer.
	CANSignalValueTypeInt
	// CANSignalValueTypeFloat defines a value of type float.
	CANSignalValueTypeFloat
	// CANSignalValueTypeEnum defines a value of type enum.
	CANSignalValueTypeEnum
)

// CANSignal represents a decoded CAN signal.
type CANSignal struct {
	// CANID is the CAN ID of the message that contains this signal.
	CANID uint32

	// Name is the name of the signal.
	Name string

	// RawValue is the raw value of the signal.
	RawValue uint64

	// Type is the type of the value of the signal.
	Type CANSignalValueType
	// ValueFlag is the value of the signal as a boolean.
	ValueFlag bool
	// ValueInt is the value of the signal as an integer.
	ValueInt int64
	// ValueFloat is the value of the signal as a float.
	ValueFloat float64
	// ValueEnum is the value of the signal as an enum.
	ValueEnum string
}

var _ msgBody = (*CANMessage)(nil)

// CANMessage represents a decoded CAN message.
// It only contains the value of the signals of every message.
type CANMessage struct {
	// Signals is the list of decoded signals.
	Signals []CANSignal
	// SignalCount is the number of decoded signals.
	SignalCount int
}

// NewCANMessage returns a new CAN message.
func NewCANMessage() *CANMessage {
	return &CANMessage{
		SignalCount: 0,
		Signals:     []CANSignal{},
	}
}

// Destroy cleans up the message.
func (cm *CANMessage) Destroy() {}

// ─── Decoder ────────────────────────────────────────────────────────────────|

type canDecoder struct {
	m map[uint32]func([]byte) []*acmelib.SignalDecoding
}

func newCANDecoder(messages []*acmelib.Message) *canDecoder {
	m := make(map[uint32]func([]byte) []*acmelib.SignalDecoding)

	for _, msg := range messages {
		m[uint32(msg.GetCANID())] = msg.SignalLayout().Decode
	}

	return &canDecoder{
		m: m,
	}
}

func (cd *canDecoder) decode(ctx context.Context, canID uint32, data []byte) []*acmelib.SignalDecoding {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	// Try to find the corresponding decode function
	fnDecode, ok := cd.m[canID]
	if !ok {
		return nil
	}
	return fnDecode(data)
}

// ─── Environment ────────────────────────────────────────────────────────────|

type canEnv struct {
	*env.BaseEnv[*CANConfig, *metrics.CanStage]

	decoder *canDecoder
}

func newCANEnv(config *CANConfig) *canEnv {
	return &canEnv{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewCanStage()),

		decoder: newCANDecoder(config.Messages),
	}
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type canWorker[T CANMessageCarrier] struct {
	worker.BaseWorker[*canEnv]
}

func newCANWorkerMaker[T CANMessageCarrier]() func() *canWorker[T] {
	return func() *canWorker[T] {
		return &canWorker[T]{}
	}
}

func (cw *canWorker[T]) Handle(ctx context.Context, msgIn *msg[T]) (*msg[*CANMessage], error) {
	ctx, span := cw.Tel.StartTrace(ctx, "handle CAN message batch")
	defer span.End()

	// Create the CAN message
	canMsg := NewCANMessage()

	rawMessages := msgIn.GetBody().GetRawMessages()
	rawMsgCount := len(rawMessages)

	for _, msg := range rawMessages {
		canID := msg.CANID

		decodings := cw.Env.decoder.decode(ctx, canID, msg.RawData)
		for _, dec := range decodings {
			sig := CANSignal{
				CANID:    canID,
				Name:     dec.Signal.Name(),
				RawValue: dec.RawValue,
			}

			switch dec.ValueType {
			case acmelib.SignalValueTypeFlag:
				sig.Type = CANSignalValueTypeFlag
				sig.ValueFlag = dec.ValueAsFlag()

			case acmelib.SignalValueTypeInt:
				sig.Type = CANSignalValueTypeInt
				sig.ValueInt = dec.ValueAsInt()

			case acmelib.SignalValueTypeUint:
				sig.Type = CANSignalValueTypeInt
				sig.ValueInt = int64(dec.ValueAsUint())

			case acmelib.SignalValueTypeFloat:
				sig.Type = CANSignalValueTypeFloat
				sig.ValueFloat = dec.ValueAsFloat()

			case acmelib.SignalValueTypeEnum:
				sig.Type = CANSignalValueTypeEnum
				sig.ValueEnum = dec.ValueAsEnum()
			}

			canMsg.Signals = append(canMsg.Signals, sig)
			canMsg.SignalCount++
		}
	}

	// Save the span in the message
	span.SetAttributes(attribute.Int("signal_count", canMsg.SignalCount))

	msgOut := message.NewMessage(canMsg)
	msgOut.SaveSpan(span)

	// Update metrics
	cw.Env.Metrics.AddCanMessages(uint(rawMsgCount))
	cw.Env.Metrics.AddCanSignals(uint(canMsg.SignalCount))

	return msgOut, nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*CANStage[CANMessageCarrier])(nil)

// CANStage is a processor stage that decodes CAN messages.
type CANStage[T CANMessageCarrier] struct {
	*stage.ProcessorStage[T, *CANMessage, *canEnv]
}

// NewCANStage returns a new CAN processor stage.
func NewCANStage[T CANMessageCarrier](inConnector msgConn[T], outConnector msgConn[*CANMessage], cfg *CANConfig) *CANStage[T] {
	env := newCANEnv(cfg)

	return &CANStage[T]{
		ProcessorStage: stage.NewProcessorStage(
			"can", inConnector, outConnector, env, newCANWorkerMaker[T](), cfg.Stage,
		),
	}
}
