package processor

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// CannelloniConfig structs contains the configuration for
// a cannelloni (encoder/decoder) stage.
type CannelloniConfig struct {
	*config.Base
}

// NewCannelloniConfig returns the default configuration for
// a cannelloni (encoder/decoder) stage.
func NewCannelloniConfig(runningMode config.StageRunningMode) *CannelloniConfig {
	return &CannelloniConfig{
		Base: config.NewBase(runningMode),
	}
}

// ─── Message ────────────────────────────────────────────────────────────────|

var _ message.ReOrderable = (*CannelloniMessage)(nil)
var _ CANMessageCarrier = (*CannelloniMessage)(nil)

const (
	// maximum number of CAN 2.0 messages (8 bytes payload) that can be sent in a single udp/ipv4/ethernet packet
	defaultCANMessageNum = 113
)

// CannelloniMessage represents a cannelloni CAN message.
type CannelloniMessage struct {
	seqNum uint8

	// Messages is the list of CAN messages contained in a cannelloni frame.
	Messages []CANRawMessage
	// MessageCount is the number of CAN messages.
	MessageCount int
}

// NewCannelloniMessage returns a new CannelloniMessage.
func NewCannelloniMessage() *CannelloniMessage {
	return &CannelloniMessage{
		MessageCount: 0,
		Messages:     make([]CANRawMessage, 0, defaultCANMessageNum),
	}
}

// Destroy cleans up the message.
func (cm *CannelloniMessage) Destroy() {}

// SetSequenceNumber sets the sequence number of the cannelloni frame.
func (cm *CannelloniMessage) SetSequenceNumber(seqNum uint8) {
	cm.seqNum = seqNum
}

// GetSequenceNumber returns the sequence number of the cannelloni frame.
func (cm *CannelloniMessage) GetSequenceNumber() uint64 {
	return uint64(cm.seqNum)
}

// GetRawMessages returns the list of CAN messages contained in the cannelloni frame.
func (cm *CannelloniMessage) GetRawMessages() []CANRawMessage {
	return cm.Messages[:cm.MessageCount]
}

// AddMessage adds a new CAN message to the cannelloni frame.
func (cm *CannelloniMessage) AddMessage(msg CANRawMessage) {
	if len(cm.Messages) == cap(cm.Messages) {
		newMessages := make([]CANRawMessage, len(cm.Messages), cap(cm.Messages)+defaultCANMessageNum)
		copy(newMessages, cm.Messages)
		cm.Messages = newMessages
	}

	cm.Messages = append(cm.Messages, msg)
	cm.MessageCount++
}

var _ msgSer = (*CannelloniEncodedMessage)(nil)

// CannelloniEncodedMessage represents a cannelloni encoded CAN message.
type CannelloniEncodedMessage struct {
	payload []byte
}

// NewCannelloniEncodedMessage returns a new CannelloniEncodedMessage.
func NewCannelloniEncodedMessage() *CannelloniEncodedMessage {
	return &CannelloniEncodedMessage{}
}

// Destroy cleans up the message.
func (cem *CannelloniEncodedMessage) Destroy() {}

// GetBytes returns the encoded bytes of a cannelloni message.
func (cem *CannelloniEncodedMessage) GetBytes() []byte {
	return cem.payload
}

type cannelloniFrameMessage struct {
	canID      uint32
	dataLen    uint8
	canFDFlags uint8
	data       []byte
}

type cannelloniFrame struct {
	version        uint8
	opCode         uint8
	sequenceNumber uint8
	messageCount   uint16
	messages       []cannelloniFrameMessage
}

// ─── Decoder ────────────────────────────────────────────────────────────────|

type cannelloniDecoder struct{}

func newCannelloniDecoder() *cannelloniDecoder {
	return &cannelloniDecoder{}
}

func (cd *cannelloniDecoder) decode(buf []byte) (*cannelloniFrame, error) {
	if buf == nil {
		return nil, errors.New("nil buffer")
	}

	if len(buf) < 5 {
		return nil, errors.New("not enough data")
	}

	f := cannelloniFrame{
		version:        buf[0],
		opCode:         buf[1],
		sequenceNumber: buf[2],
		messageCount:   binary.BigEndian.Uint16(buf[3:5]),
	}

	f.messages = make([]cannelloniFrameMessage, f.messageCount)
	pos := 5
	for i := uint16(0); i < f.messageCount; i++ {
		n, err := cd.decodeMessage(buf[pos:], &f.messages[i])
		if err != nil {
			return nil, err
		}

		pos += n
	}

	return &f, nil
}

func (cd *cannelloniDecoder) decodeMessage(buf []byte, msg *cannelloniFrameMessage) (int, error) {
	if len(buf) < 5 {
		return 0, errors.New("not enough data")
	}

	n := 5

	msg.canID = binary.BigEndian.Uint32(buf[0:4])

	isCANFD := false
	tmpDataLen := buf[4]
	if tmpDataLen&0x80 == 0x80 {
		isCANFD = true
	}

	if isCANFD {
		if len(buf) < 6 {
			return 0, errors.New("not enough data")
		}

		tmpDataLen &= 0x7f
		msg.canFDFlags = buf[5]
		n++
	}

	msg.dataLen = tmpDataLen

	if len(buf) < n+int(tmpDataLen) {
		return 0, errors.New("not enough data for message content")
	}

	msg.data = make([]byte, tmpDataLen)

	copy(msg.data, buf[n:n+int(tmpDataLen)])
	n += int(msg.dataLen)

	return n, nil
}

// ─── Decoder - Environment ──────────────────────────────────────────────────|

type cannelloniDecoderEnv[T msgSer] struct {
	*env.BaseEnv[*CannelloniConfig, *metrics.EmptyMetrics]

	decoder *cannelloniDecoder
}

func newCannelloniDecoderEnv[T msgSer](config *CannelloniConfig) *cannelloniDecoderEnv[T] {
	return &cannelloniDecoderEnv[T]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		decoder: newCannelloniDecoder(),
	}
}

// ─── Decoder - Worker ───────────────────────────────────────────────────────|

type cannelloniDecoderWorker[T msgSer] struct {
	worker.BaseWorker[*cannelloniDecoderEnv[T]]
}

func newCannelloniDecoderWorkerMaker[T msgSer]() func() *cannelloniDecoderWorker[T] {
	return func() *cannelloniDecoderWorker[T] {
		return &cannelloniDecoderWorker[T]{}
	}
}

func (cdw *cannelloniDecoderWorker[T]) Handle(ctx context.Context, msgIn *msg[T]) (*msg[*CannelloniMessage], error) {
	cdw.Tel.LogDebug("handle cannelloni frame")

	_, span := cdw.Tel.StartTrace(ctx, "handle cannelloni frame")
	defer span.End()

	// Decode the frame
	f, err := cdw.Env.decoder.decode(msgIn.GetBody().GetBytes())
	if err != nil {
		return nil, err
	}

	// Create the cannelloni message with the decoded frame data
	cannelloniMsg := NewCannelloniMessage()
	cannelloniMsg.seqNum = f.sequenceNumber

	for _, tmpMsg := range f.messages {
		cannelloniMsg.AddMessage(
			CANRawMessage{
				CANID:   tmpMsg.canID,
				DataLen: int(tmpMsg.dataLen),
				RawData: tmpMsg.data,
			},
		)
	}

	// Save the span into the message
	span.SetAttributes(attribute.Int("message_count", cannelloniMsg.MessageCount))

	msgOut := message.NewMessage(cannelloniMsg)
	msgOut.SaveSpan(span)

	return msgOut, nil
}

// ─── Decoder - Stage ────────────────────────────────────────────────────────|

var _ stage.Stage = (*CannelloniDecoderStage[msgSer])(nil)

// CannelloniDecoderStage is a processor stage that decodes
// cannelloni messages into CAN messages.
type CannelloniDecoderStage[T msgSer] struct {
	*stage.ProcessorStage[T, *CannelloniMessage, *cannelloniDecoderEnv[T]]
}

// NewCannelloniDecoderStage returns a new cannelloni decoder processor stage.
func NewCannelloniDecoderStage[T msgSer](
	inConnector msgConn[T], outConnector msgConn[*CannelloniMessage], cfg *CannelloniConfig,
) *CannelloniDecoderStage[T] {

	env := newCannelloniDecoderEnv[T](cfg)

	return &CannelloniDecoderStage[T]{
		ProcessorStage: stage.NewProcessorStage(
			"cannelloni_decoder", inConnector, outConnector,
			env, newCannelloniDecoderWorkerMaker[T](), cfg.Stage,
		),
	}
}

// ─── Encoder ────────────────────────────────────────────────────────────────|

type cannelloniEncoder struct{}

func newCannelloniEncoder() *cannelloniEncoder {
	return &cannelloniEncoder{}
}

func (ce *cannelloniEncoder) encode(frame *cannelloniFrame) []byte {
	totMsgSize := int(frame.messageCount * 5)
	for _, msg := range frame.messages {
		if msg.canFDFlags != 0 {
			totMsgSize += int(msg.dataLen) + 1
			continue
		}

		totMsgSize += int(msg.dataLen)
	}

	buf := make([]byte, 5+totMsgSize)

	buf[0] = frame.version
	buf[1] = frame.opCode
	buf[2] = frame.sequenceNumber
	binary.BigEndian.PutUint16(buf[3:5], frame.messageCount)

	pos := 5
	for _, msg := range frame.messages {
		n := ce.encodeMessage(&msg, buf[pos:])
		pos += n
	}

	return buf
}

func (ce *cannelloniEncoder) encodeMessage(msg *cannelloniFrameMessage, buf []byte) int {
	n := 5

	binary.BigEndian.PutUint32(buf[0:4], msg.canID)

	buf[4] = msg.dataLen

	if msg.canFDFlags != 0 {
		buf[4] |= 0x80
		buf[5] = msg.canFDFlags
		n++
	}

	tmpDataLen := int(msg.dataLen)
	copy(buf[n:n+tmpDataLen], msg.data)
	n += tmpDataLen

	return n
}

// ─── Encoder - Environment ──────────────────────────────────────────────────|

type cannelloniEncoderEnv struct {
	*env.BaseEnv[*CannelloniConfig, *metrics.EmptyMetrics]

	encoder *cannelloniEncoder
}

func newCannelloniEncoderEnv(config *CannelloniConfig) *cannelloniEncoderEnv {
	return &cannelloniEncoderEnv{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		encoder: newCannelloniEncoder(),
	}
}

// ─── Encoder - Worker ───────────────────────────────────────────────────────|

type cannelloniEncoderWorker struct {
	worker.BaseWorker[*cannelloniEncoderEnv]
}

func newCannelloniEncoderWorkerMaker() func() *cannelloniEncoderWorker {
	return func() *cannelloniEncoderWorker {
		return &cannelloniEncoderWorker{}
	}
}

func (cew *cannelloniEncoderWorker) Handle(ctx context.Context, msgIn *msg[*CannelloniMessage]) (*msg[*CannelloniEncodedMessage], error) {
	cew.Tel.LogDebug("handle cannelloni frame")

	// Extract the span context from the input message
	_, span := cew.Tel.StartTrace(ctx, "handle cannelloni frame")
	defer span.End()

	msgVal := msgIn.GetBody()

	f := &cannelloniFrame{
		version:        1,
		opCode:         0,
		sequenceNumber: msgVal.seqNum,
		messageCount:   uint16(msgVal.MessageCount),
		messages:       make([]cannelloniFrameMessage, 0, msgVal.MessageCount),
	}

	for _, msg := range msgVal.Messages {
		f.messages = append(f.messages, cannelloniFrameMessage{
			canID:   msg.CANID,
			dataLen: uint8(msg.DataLen),
			data:    msg.RawData,
		})
	}

	// Encode into a cannelloni message
	encodedMsg := NewCannelloniEncodedMessage()
	encodedMsg.payload = cew.Env.encoder.encode(f)

	msgOut := message.NewMessage(encodedMsg)
	msgOut.SaveSpan(span)

	return msgOut, nil
}

// ─── Encoder - Stage ────────────────────────────────────────────────────────|

var _ stage.Stage = (*CannelloniEncoderStage)(nil)

// CannelloniEncoderStage is a processor stage that encodes
// CAN messages into cannelloni messages.
type CannelloniEncoderStage struct {
	*stage.ProcessorStage[*CannelloniMessage, *CannelloniEncodedMessage, *cannelloniEncoderEnv]
}

// NewCannelloniEncoderStage returns a new cannelloni encoder processor stage.
func NewCannelloniEncoderStage(
	inConnector msgConn[*CannelloniMessage], outConnector msgConn[*CannelloniEncodedMessage], cfg *CannelloniConfig,
) *CannelloniEncoderStage {

	env := newCannelloniEncoderEnv(cfg)

	return &CannelloniEncoderStage{
		ProcessorStage: stage.NewProcessorStage(
			"cannelloni_encoder", inConnector, outConnector,
			env, newCannelloniEncoderWorkerMaker(), cfg.Stage,
		),
	}
}
