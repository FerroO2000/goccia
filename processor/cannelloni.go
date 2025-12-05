package processor

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/pool"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

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

///////////////
//  MESSAGE  //
///////////////

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

func newCannelloniMessage() *CannelloniMessage {
	return &CannelloniMessage{
		MessageCount: 0,
		Messages:     make([]CANRawMessage, defaultCANMessageNum),
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
	cm.Messages = append(cm.Messages, msg)
	cm.MessageCount++
}

var _ msgSer = (*CannelloniEncodedMessage)(nil)

// CannelloniEncodedMessage represents a cannelloni encoded CAN message.
type CannelloniEncodedMessage struct {
	payload []byte
}

func newCannelloniEncodedMessage() *CannelloniEncodedMessage {
	return &CannelloniEncodedMessage{}
}

// Destroy cleans up the message.
func (cem *CannelloniEncodedMessage) Destroy() {}

// GetBytes returns the encoded bytes of a cannelloni message.
func (cem *CannelloniEncodedMessage) GetBytes() []byte {
	return cem.payload
}

///////////////
//  DECODER  //
///////////////

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

///////////////
//  ENCODER  //
///////////////

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

//////////////
//  WORKER  //
//////////////

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

type cannelloniDecoderWorker[T msgSer] struct {
	pool.BaseWorker

	decoder *cannelloniDecoder
}

func newCannelloniDecoderWorkerInstMaker[T msgSer]() workerInstanceMaker[any, T, *CannelloniMessage] {
	return func() workerInstance[any, T, *CannelloniMessage] {
		return &cannelloniDecoderWorker[T]{}
	}
}

func (cdw *cannelloniDecoderWorker[T]) Init(_ context.Context, _ any) error {
	cdw.decoder = newCannelloniDecoder()

	return nil
}

func (cdw *cannelloniDecoderWorker[T]) Handle(ctx context.Context, msgIn *msg[T]) (*msg[*CannelloniMessage], error) {
	_, span := cdw.Tel.NewTrace(ctx, "handle cannelloni frame")
	defer span.End()

	// Decode the frame
	f, err := cdw.decoder.decode(msgIn.GetEnvelope().GetBytes())
	if err != nil {
		return nil, err
	}

	// Create the cannelloni message with the decoded frame data
	cannelloniMsg := newCannelloniMessage()

	cannelloniMsg.seqNum = f.sequenceNumber

	messageCount := len(f.messages)
	cannelloniMsg.MessageCount = messageCount
	if messageCount > defaultCANMessageNum {
		cannelloniMsg.Messages = make([]CANRawMessage, messageCount)
	}

	for idx, tmpMsg := range f.messages {
		cannelloniMsg.Messages[idx] = CANRawMessage{
			CANID:   tmpMsg.canID,
			DataLen: int(tmpMsg.dataLen),
			RawData: tmpMsg.data,
		}
	}

	// Save the span into the message
	span.SetAttributes(attribute.Int("message_count", messageCount))

	msgOut := message.NewMessage(cannelloniMsg)
	msgOut.SaveSpan(span)

	return msgOut, nil
}

func (cdw *cannelloniDecoderWorker[T]) Close(_ context.Context) error {
	return nil
}

type cannelloniEncoderWorker struct {
	pool.BaseWorker

	encoder *cannelloniEncoder
}

func newCannelloniEncoderWorkerInstMaker() workerInstanceMaker[any, *CannelloniMessage, *CannelloniEncodedMessage] {
	return func() workerInstance[any, *CannelloniMessage, *CannelloniEncodedMessage] {
		return &cannelloniEncoderWorker{}
	}
}

func (cew *cannelloniEncoderWorker) Init(_ context.Context, _ any) error {
	cew.encoder = newCannelloniEncoder()

	return nil
}

func (cew *cannelloniEncoderWorker) Handle(ctx context.Context, msgIn *msg[*CannelloniMessage]) (*msg[*CannelloniEncodedMessage], error) {
	// Extract the span context from the input message
	_, span := cew.Tel.NewTrace(ctx, "handle cannelloni frame")
	defer span.End()

	msgVal := msgIn.GetEnvelope()

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
	encodedMsg := newCannelloniEncodedMessage()
	encodedMsg.payload = cew.encoder.encode(f)

	msgOut := message.NewMessage(encodedMsg)
	msgOut.SaveSpan(span)

	return msgOut, nil
}

func (cew *cannelloniEncoderWorker) Close(_ context.Context) error {
	return nil
}

/////////////
//  STAGE  //
/////////////

// CannelloniDecoderStage is a processor stage that decodes
// cannelloni messages into CAN messages.
type CannelloniDecoderStage[T msgSer] struct {
	stage[any, T, *CannelloniMessage, *CannelloniConfig]
}

// NewCannelloniDecoderStage returns a new cannelloni decoder processor stage.
func NewCannelloniDecoderStage[T msgSer](inputConnector msgConn[T], outputConnector msgConn[*CannelloniMessage], cfg *CannelloniConfig) *CannelloniDecoderStage[T] {
	return &CannelloniDecoderStage[T]{
		stage: newStage(
			"cannelloni", inputConnector, outputConnector, newCannelloniDecoderWorkerInstMaker[T](), cfg,
		),
	}
}

// Init initializes the stage.
func (cds *CannelloniDecoderStage[T]) Init(ctx context.Context) error {
	return cds.stage.Init(ctx, nil)
}

// CannelloniEncoderStage is a processor stage that encodes
// CAN messages into cannelloni messages.
type CannelloniEncoderStage struct {
	stage[any, *CannelloniMessage, *CannelloniEncodedMessage, *CannelloniConfig]
}

// NewCannelloniEncoderStage returns a new cannelloni encoder processor stage.
func NewCannelloniEncoderStage(
	inputConnector msgConn[*CannelloniMessage], outputConnector msgConn[*CannelloniEncodedMessage], cfg *CannelloniConfig,
) *CannelloniEncoderStage {

	return &CannelloniEncoderStage{
		stage: newStage(
			"cannelloni", inputConnector, outputConnector, newCannelloniEncoderWorkerInstMaker(), cfg,
		),
	}
}

// Init initializes the stage.
func (ces *CannelloniEncoderStage) Init(ctx context.Context) error {
	return ces.stage.Init(ctx, nil)
}
