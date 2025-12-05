package processor

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/pool"
	"github.com/squadracorsepolito/acmelib"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

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

///////////////
//  MESSAGE  //
///////////////

// CANMessageCarrier interface defines the common methods
// for all message types that carry CAN messages.
type CANMessageCarrier interface {
	msgEnv

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

var _ msgEnv = (*CANMessage)(nil)

// CANMessage represents a decoded CAN message.
// It only contains the value of the signals of every message.
type CANMessage struct {
	// Signals is the list of decoded signals.
	Signals []CANSignal
	// SignalCount is the number of decoded signals.
	SignalCount int
}

func newCANMessage() *CANMessage {
	return &CANMessage{
		SignalCount: 0,
		Signals:     []CANSignal{},
	}
}

// Destroy cleans up the message.
func (cm *CANMessage) Destroy() {}

///////////////
//  DECODER  //
///////////////

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

	fn, ok := cd.m[canID]
	if !ok {
		return nil
	}
	return fn(data)
}

////////////////////////
//  WORKER ARGUMENTS  //
////////////////////////

type canWorkerArgs struct {
	decoder *canDecoder
}

func newCANWorkerArgs(decoder *canDecoder) *canWorkerArgs {
	return &canWorkerArgs{
		decoder: decoder,
	}
}

//////////////////////
//  WORKER METRICS  //
//////////////////////

type canWorkerMetrics struct {
	once sync.Once

	canMessages atomic.Int64
	canSignals  atomic.Int64
}

var canWorkerMetricsInst = &canWorkerMetrics{}

func (cwm *canWorkerMetrics) init(tel *internal.Telemetry) {
	cwm.once.Do(func() {
		cwm.initMetrics(tel)
	})
}

func (cwm *canWorkerMetrics) initMetrics(tel *internal.Telemetry) {
	tel.NewCounter("can_messages", func() int64 { return cwm.canMessages.Load() })
	tel.NewCounter("can_signals", func() int64 { return cwm.canSignals.Load() })
}

func (cwm *canWorkerMetrics) addCANMessages(amount int) {
	cwm.canMessages.Add(int64(amount))
}

func (cwm *canWorkerMetrics) addCANSignals(amount int) {
	cwm.canSignals.Add(int64(amount))
}

/////////////////////////////
//  WORKER IMPLEMENTATION  //
/////////////////////////////

type canWorker[T CANMessageCarrier] struct {
	pool.BaseWorker

	decoder *canDecoder

	metrics *canWorkerMetrics
}

func newCANWorkerInstMaker[T CANMessageCarrier]() workerInstanceMaker[*canWorkerArgs, T, *CANMessage] {
	return func() workerInstance[*canWorkerArgs, T, *CANMessage] {
		return &canWorker[T]{
			metrics: canWorkerMetricsInst,
		}
	}
}

func (cw *canWorker[T]) Init(_ context.Context, args *canWorkerArgs) error {
	cw.decoder = args.decoder

	cw.metrics.init(cw.Tel)

	return nil
}

func (cw *canWorker[T]) Handle(ctx context.Context, msgIn *msg[T]) (*msg[*CANMessage], error) {
	ctx, span := cw.Tel.NewTrace(ctx, "handle CAN message batch")
	defer span.End()

	// Create the CAN message
	canMsg := newCANMessage()

	rawMessages := msgIn.GetEnvelope().GetRawMessages()
	rawMsgCount := len(rawMessages)

	for _, msg := range rawMessages {
		canID := msg.CANID

		decodings := cw.decoder.decode(ctx, canID, msg.RawData)
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
	cw.metrics.addCANMessages(rawMsgCount)
	cw.metrics.addCANSignals(canMsg.SignalCount)

	return msgOut, nil
}

func (cw *canWorker[T]) Close(_ context.Context) error {
	return nil
}

/////////////
//  STAGE  //
/////////////

// CANStage is a processor stage that decodes CAN messages.
type CANStage[T CANMessageCarrier] struct {
	stage[*canWorkerArgs, T, *CANMessage, *CANConfig]
}

// NewCANStage returns a new CAN processor stage.
func NewCANStage[T CANMessageCarrier](inputConnector msgConn[T], outputConnector msgConn[*CANMessage], cfg *CANConfig) *CANStage[T] {
	return &CANStage[T]{
		stage: newStage(
			"can", inputConnector, outputConnector, newCANWorkerInstMaker[T](), cfg,
		),
	}
}

// Init initializes the stage.
func (cs *CANStage[T]) Init(ctx context.Context) error {
	decoder := newCANDecoder(cs.Config().Messages)

	return cs.stage.Init(ctx, newCANWorkerArgs(decoder))
}
