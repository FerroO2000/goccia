package ingress

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/ingress/metrics"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/rb"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Endianess defines the endianness of a slice of bytes.
type Endianess uint8

const (
	// LittleEndian defines little endianess.
	LittleEndian Endianess = iota
	// BigEndian defines big endianess.
	BigEndian
)

// TCPFramingMode defines the framing mode to use.
type TCPFramingMode uint8

const (
	// TCPFramingModeDelimited will use delimited messages.
	TCPFramingModeDelimited TCPFramingMode = iota
	// TCPFramingModeLengthPrefixed will use length-prefixed messages.
	TCPFramingModeLengthPrefixed
)

// Default values for the TCP ingress stage configuration.
const (
	DefaultTCPConfigIPAddr          = "0.0.0.0"
	DefaultTCPConfigPort            = 20_000
	DefaultTCPConfigBufferSize      = 4096
	DefaultTCPConfigReadTimeout     = 10 * time.Second
	DefaultTCPConfigFramingMode     = TCPFramingModeDelimited
	DefaultTCPConfigMaxMessageSize  = 4 << 20
	DefaultTCPConfigOutputQueueSize = 512
	DefaultTCPConfigHeaderLen       = 16
)

// DefaultTCPConfigDelimiter is the default delimiter for delimited messages.
var DefaultTCPConfigDelimiter = []byte("\r\n")

// TCPConfig structs contains the configuration for the TCP ingress stage.
type TCPConfig struct {
	// IPAddr is the IP address of the server to listen on.
	IPAddr string

	// Port is the port to listen on.
	Port uint16

	// BufferSize is the size of the buffer to use for reading
	// from the connection.
	BufferSize uint16

	// ReadTimeout is the timeout for reading from a connection.
	ReadTimeout time.Duration

	// FramingMode is the framing mode to use.
	// It basically defines how the messages are separated.
	FramingMode TCPFramingMode

	// MaxMessageSize is the maximum size of a message.
	// If the accumulator that is holding the message
	// gets bigger, the connection is closed.
	MaxMessageSize int

	// Delimiter is the delimiter to use to separate messages
	// when the FramingMode is TCPFramingModeDelimited.
	Delimiter []byte

	// HeaderLen is the length of the header in the context
	// of the TCPFramingModeLengthPrefixed mode.
	HeaderLen int

	// MessageLengthFieldLen is the length of the message length field
	// when FramingMode is TCPFramingModeLengthPrefixed.
	MessageLengthFieldLen int

	// MessageLengthFieldOffset is the offset in the header
	// of the message length field when FramingMode is TCPFramingModeLengthPrefixed.
	MessageLengthFieldOffset int

	// MessageLengthFieldEndianess is the endianess (byte order)
	// of the message length field when FramingMode is TCPFramingModeLengthPrefixed.
	MessageLengthFieldEndianess Endianess

	// OutputQueueSize is the size of the output queue
	// placed between the TCP stage and the output connector.
	// It is used to convey messages coming from the connection goroutines
	// to the output connector.
	OutputQueueSize int
}

// NewTCPConfig returns a default TCPConfig.
func NewTCPConfig() TCPConfig {
	return TCPConfig{
		IPAddr:          DefaultTCPConfigIPAddr,
		Port:            DefaultTCPConfigPort,
		ReadTimeout:     DefaultTCPConfigReadTimeout,
		FramingMode:     DefaultTCPConfigFramingMode,
		MaxMessageSize:  DefaultTCPConfigMaxMessageSize,
		Delimiter:       DefaultTCPConfigDelimiter,
		OutputQueueSize: DefaultTCPConfigOutputQueueSize,
	}
}

// Validate checks the configuration.
func (c *TCPConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotEmpty(ac, "IPAddr", &c.IPAddr, DefaultTCPConfigIPAddr)

	config.CheckNotZero(ac, "Port", &c.Port, DefaultTCPConfigPort)

	config.CheckNotNegative(ac, "ReadTimeout", &c.ReadTimeout, DefaultTCPConfigReadTimeout)
	config.CheckNotZero(ac, "ReadTimeout", &c.ReadTimeout, DefaultTCPConfigReadTimeout)

	config.CheckNotNegative(ac, "MaxMessageSize", &c.MaxMessageSize, DefaultTCPConfigMaxMessageSize)
	config.CheckNotZero(ac, "MaxMessageSize", &c.MaxMessageSize, DefaultTCPConfigMaxMessageSize)

	config.CheckLen(ac, "Delimiter", &c.Delimiter, DefaultTCPConfigDelimiter)

	config.CheckNotNegative(ac, "OutputQueueSize", &c.OutputQueueSize, DefaultTCPConfigOutputQueueSize)
	config.CheckNotZero(ac, "OutputQueueSize", &c.OutputQueueSize, DefaultTCPConfigOutputQueueSize)

	if c.FramingMode == TCPFramingModeDelimited {
		return
	}

	// Check configuration when framing mode is length-prefixed
	config.CheckNotNegative(ac, "HeaderLen", &c.HeaderLen, DefaultTCPConfigHeaderLen)
	config.CheckNotZero(ac, "HeaderLen", &c.HeaderLen, DefaultTCPConfigHeaderLen)

	config.CheckNotNegative(ac, "MessageLengthFieldLen", &c.MessageLengthFieldLen, c.HeaderLen)
	config.CheckNotGreaterThan(ac, "MessageLengthFieldLen", "HeaderLen", &c.MessageLengthFieldLen, c.HeaderLen)

	config.CheckNotNegative(ac, "MessageLengthFieldOffset", &c.MessageLengthFieldOffset, 0)
	config.CheckNotGreaterThan(ac,
		"MessageLengthFieldOffset", "HeaderLen-MessageLengthFieldLen",
		&c.MessageLengthFieldOffset, c.HeaderLen-c.MessageLengthFieldLen,
	)
}

// ─── Message ────────────────────────────────────────────────────────────────|

var _ msgSer = (*TCPMessage)(nil)

// TCPMessage represents a TCP message.
type TCPMessage struct {
	// RemoteAddr is the remote address of the connection.
	RemoteAddr string
	// Message is the message payload.
	Message []byte
	// MessageSize is the size of the message payload.
	MessageSize int
}

// NewTCPMessage returns a new TCPMessage.
func NewTCPMessage() *TCPMessage {
	return &TCPMessage{}
}

// Destroy cleans up the message.
func (tm *TCPMessage) Destroy() {}

// GetBytes returns the bytes of the TCP message.
func (tm *TCPMessage) GetBytes() []byte {
	return tm.Message
}

// ─── Environment ────────────────────────────────────────────────────────────|

type tcpEnv struct {
	*env.BaseEnv[*TCPConfig, *metrics.TcpStage]

	bufPool sync.Pool

	listener *net.TCPListener

	bufferSize  int
	readTimeout time.Duration

	// Framing
	framingMode TCPFramingMode
	maxMsgSize  int
	// Delimited
	delimiter    []byte
	delimiterLen int
	// Lenght Prefixed
	headerLen            int
	msgLenFieldOffset    int
	msgLenFieldLen       int
	msgLenFieldParseLen  int
	msgLenFieldEndianess Endianess
}

func newTCPEnv(config *TCPConfig) *tcpEnv {
	return &tcpEnv{
		BaseEnv: env.NewIngressEnv(config, metrics.NewTcpStage()),
	}
}

func (te *tcpEnv) Init(ctx context.Context) error {
	// Initialize the base environment first (config validation)
	if err := te.BaseEnv.Init(ctx); err != nil {
		return err
	}

	te.initBufferPool()

	if err := te.initListener(); err != nil {
		return err
	}

	te.initConfig()

	return nil
}

func (te *tcpEnv) initBufferPool() {
	te.bufPool = sync.Pool{
		New: func() any {
			return make([]byte, te.Config.BufferSize)
		},
	}
}

func (te *tcpEnv) initListener() error {
	parsedAddr, err := netip.ParseAddr(te.Config.IPAddr)
	if err != nil {
		return err
	}

	addr := netip.AddrPortFrom(parsedAddr, te.Config.Port)
	listener, err := net.ListenTCP("tcp", net.TCPAddrFromAddrPort(addr))
	if err != nil {
		return err
	}

	te.listener = listener

	return nil
}

func (te *tcpEnv) initConfig() {
	te.bufferSize = int(te.Config.BufferSize)
	te.readTimeout = te.Config.ReadTimeout

	te.framingMode = te.Config.FramingMode
	te.maxMsgSize = te.Config.MaxMessageSize

	// Delimited
	te.delimiter = te.Config.Delimiter
	te.delimiterLen = len(te.delimiter)

	// Length Prefixed
	te.headerLen = te.Config.HeaderLen
	te.msgLenFieldOffset = te.Config.MessageLengthFieldOffset
	te.msgLenFieldLen = te.Config.MessageLengthFieldLen

	msgLenFieldParseLen := te.Config.MessageLengthFieldLen
	switch msgLenFieldParseLen {
	case 3:
		msgLenFieldParseLen = 4
	case 5, 6, 7:
		msgLenFieldParseLen = 8
	}
	te.msgLenFieldParseLen = msgLenFieldParseLen

	te.msgLenFieldEndianess = te.Config.MessageLengthFieldEndianess
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*tcpEnv] = (*tcpRunner)(nil)

type tcpRunner struct {
	*tcpEnv

	outConnector msgConn[*TCPMessage]
	runDone      chan struct{}

	connWG    *sync.WaitGroup
	connFanIn *rb.RingBuffer[*msg[*TCPMessage]]
}

func newTCPRunner(outConnector msgConn[*TCPMessage]) *tcpRunner {
	return &tcpRunner{
		outConnector: outConnector,
		runDone:      make(chan struct{}),

		connWG: &sync.WaitGroup{},
	}
}

func (tr *tcpRunner) SetEnvironment(env *tcpEnv) {
	tr.tcpEnv = env
}

func (tr *tcpRunner) Init(_ context.Context) error {
	fanInBufSize := uint64(tr.Config.OutputQueueSize)
	tr.connFanIn = rb.NewRingBuffer[*msg[*TCPMessage]](fanInBufSize, rb.BufferKindSPMC)

	return nil
}

func (tr *tcpRunner) runIO(ctx context.Context) {
	defer tr.outConnector.Close()

	for {
		msg, err := tr.connFanIn.Read(ctx)
		if err != nil {
			return
		}

		if err := tr.outConnector.Write(msg); err != nil {
			msg.Destroy()
		}
	}
}

func (tr *tcpRunner) Run(ctx context.Context) {
	defer close(tr.runDone)

	go func() {
		<-ctx.Done()
		tr.listener.Close()
	}()

	go tr.runIO(ctx)

	for {
		conn, err := tr.listener.Accept()
		if err != nil {
			// Check if the error is because the context is done
			if ctx.Err() != nil {
				return
			}

			tr.Tel.LogWarn("failed to accept connection", err)
			continue
		}

		// Spawn a goroutine to handle the connection
		tr.connWG.Add(1)
		go tr.handleConn(ctx, conn)
	}
}

func (tr *tcpRunner) handleConn(ctx context.Context, conn net.Conn) {
	defer tr.connWG.Done()

	defer conn.Close()

	// Channel to notify when the connection is closed normally
	connClosed := make(chan struct{})
	defer close(connClosed)

	// Close the connection when the context is done
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-connClosed:
			// Connection closed normally
		}
	}()

	// Handle the open connections metric
	tr.Metrics.IncrementOpenConnections()
	defer tr.Metrics.DecrementOpenConnections()

	// Get the buffer from the pool
	buf := tr.bufPool.Get().([]byte)
	defer tr.bufPool.Put(buf)

	// Preallocate the accumulator
	accBaseCap := min(4*tr.bufferSize, tr.maxMsgSize)
	acc := make([]byte, 0, accBaseCap)

	minAccLen := 0
	switch tr.framingMode {
	case TCPFramingModeDelimited:
		minAccLen = tr.delimiterLen
	case TCPFramingModeLengthPrefixed:
		minAccLen = tr.headerLen
	}

loop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set the read deadline
		conn.SetReadDeadline(time.Now().Add(tr.readTimeout))

		// Read the TCP stream
		n, err := conn.Read(buf)
		if err != nil {
			// Check if the connection has been closed by the client,
			// if so, close the server connection
			if errors.Is(err, io.EOF) {
				return
			}

			// Check if the connection is closed and if the context is done
			// return without re-closing the connection
			if errors.Is(err, net.ErrClosed) {
				select {
				case <-ctx.Done():
					return
				default:
				}
			}

			// For any other error, break the loop and close the server connection.
			// This is likely be caused by the read deadline being exceeded.
			tr.Tel.LogError("failed to read connection", err)
			return
		}

		// Append the new bytes to the accumulator
		acc = append(acc, buf[:n]...)

		// Prevent accumulator from growing too large
		if len(acc) > tr.maxMsgSize {
			tr.Tel.LogWarn("message too large, closing connection")
			return
		}

		for {
			accLen := len(acc)

			// If the accumulator is smaller than the minimum length,
			// continue reading the TCP stream
			if accLen < minAccLen {
				continue loop
			}

			// Get the length of the message.
			msgLen := 0
			totLen := 0
			switch tr.framingMode {
			case TCPFramingModeDelimited:
				// Search for the delimiter
				msgLen = bytes.Index(acc, tr.delimiter)
				totLen = msgLen + tr.delimiterLen

			case TCPFramingModeLengthPrefixed:
				msgLen = tr.parseHeader(acc[:tr.headerLen])
				totLen = msgLen + tr.headerLen
			}

			if msgLen == -1 || accLen < totLen {
				// If the message length is not found or the accumulator is too small,
				// break the loop and continue reading the TCP stream
				break
			}

			// Extract the message
			msg := acc[:totLen]

			// Handle the message and send the result to the output connector
			outMsg := tr.handleMessage(ctx, msg)
			outMsg.GetBody().RemoteAddr = conn.RemoteAddr().String()
			if err := tr.connFanIn.Write(outMsg); err != nil {
				outMsg.Destroy()
				tr.Tel.LogError("failed to write message to fan in connector", err)
			}

			// Remove the message from the accumulator
			acc = acc[totLen:]

			// Check if the accumulator should be reset
			if len(acc) == 0 && cap(acc) > accBaseCap {
				acc = make([]byte, 0, accBaseCap)
				break
			}
		}

		// Prevent accumulator from growing too large, as before
		if len(acc) > tr.maxMsgSize {
			tr.Tel.LogWarn("message too large, closing connection")
			return
		}
	}
}

func (tr *tcpRunner) parseHeader(header []byte) int {
	if len(header) < tr.headerLen {
		return -1
	}

	msgLenField := header[tr.msgLenFieldOffset : tr.msgLenFieldOffset+tr.msgLenFieldLen]

	buf := msgLenField
	// Check if the message length field should be extended
	if tr.msgLenFieldLen != tr.msgLenFieldParseLen {
		buf = make([]byte, tr.msgLenFieldParseLen)

		switch tr.msgLenFieldEndianess {
		case LittleEndian:
			copy(buf, msgLenField)
		case BigEndian:
			copy(buf[tr.msgLenFieldParseLen-tr.msgLenFieldLen:], msgLenField)
		}
	}

	switch tr.msgLenFieldEndianess {
	case LittleEndian:
		return tr.parseLittleEndianMsgLen(buf)
	case BigEndian:
		return tr.parseBigEndianMsgLen(buf)
	}

	return 0
}

func (tr *tcpRunner) parseLittleEndianMsgLen(buf []byte) int {
	switch len(buf) {
	case 1:
		return int(buf[0])
	case 2:
		return int(binary.LittleEndian.Uint16(buf))
	case 4:
		return int(binary.LittleEndian.Uint32(buf))
	case 8:
		return int(binary.LittleEndian.Uint64(buf))
	default:
		return -1
	}
}

func (tr *tcpRunner) parseBigEndianMsgLen(buf []byte) int {
	switch len(buf) {
	case 1:
		return int(buf[0])
	case 2:
		return int(binary.BigEndian.Uint16(buf))
	case 4:
		return int(binary.BigEndian.Uint32(buf))
	case 8:
		return int(binary.BigEndian.Uint64(buf))
	default:
		return -1
	}
}

func (tr *tcpRunner) handleMessage(ctx context.Context, rawMsg []byte) *msg[*TCPMessage] {
	// Create the trace for the incoming message
	_, span := tr.Tel.StartTrace(ctx, "receive TCP message")
	defer span.End()

	// Create the TCP message
	tcpMsg := NewTCPMessage()

	// Extract the payload from the buffer
	msgSize := len(rawMsg)
	tcpMsg.MessageSize = msgSize
	tcpMsg.Message = make([]byte, msgSize)
	copy(tcpMsg.Message, rawMsg)

	msg := message.NewMessage(tcpMsg)

	// Set the receive time and the timestamp
	recvTime := time.Now()
	msg.SetReceiveTime(recvTime)
	msg.SetTimestamp(recvTime)

	// Save the span into the message
	span.SetAttributes(attribute.Int("payload_size", msgSize))
	msg.SaveSpan(span)

	// Update metrics
	tr.Metrics.AddReceivedBytes(uint(msgSize))
	tr.Metrics.IncrementReceivedMessages()

	return msg
}

func (tr *tcpRunner) Close(_ context.Context) {
	<-tr.runDone

	tr.connWG.Wait()

	tr.connFanIn.Close()
}

func (tr *tcpRunner) Inputs() []uintptr {
	return []uintptr{}
}

func (tr *tcpRunner) Outputs() []uintptr {
	return []uintptr{connector.GetConnectorID(tr.outConnector)}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// TCPStage is an ingress stage that reads TCP connections and extracts messages.
type TCPStage struct {
	*stage.IngressStage[*TCPMessage, *tcpEnv]
}

// NewTCPStage returns a new TCP stage.
func NewTCPStage(outConnector msgConn[*TCPMessage], cfg *TCPConfig) *TCPStage {
	return &TCPStage{
		IngressStage: stage.NewIngressStageFromRunner[*TCPMessage](
			"tcp", newTCPEnv(cfg), newTCPRunner(outConnector),
		),
	}
}
