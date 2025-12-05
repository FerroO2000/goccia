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
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/pool"
	"go.opentelemetry.io/otel/attribute"
)

const (
	tcpBufSize = 4096
)

//////////////
//  CONFIG  //
//////////////

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

///////////////
//  MESSAGE  //
///////////////

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

func newTCPMessage() *TCPMessage {
	return &TCPMessage{}
}

// Destroy cleans up the message.
func (tm *TCPMessage) Destroy() {}

// GetBytes returns the bytes of the TCP message.
func (tm *TCPMessage) GetBytes() []byte {
	return tm.Message
}

//////////////
//  SOURCE  //
//////////////

var _ source[*TCPMessage] = (*tcpSource)(nil)

type tcpSourceConfig struct {
	fanInBufferSize int

	readTimeout time.Duration

	framingMode TCPFramingMode
	maxMsgSize  int

	delimiter []byte

	headerLen            int
	msgLenFieldOffset    int
	msgLenFieldLen       int
	msgLenFieldEndianess Endianess
}

type tcpSource struct {
	tel *internal.Telemetry

	fanIn *pool.FanIn[*msg[*TCPMessage]]

	wg *sync.WaitGroup

	bufPool sync.Pool

	listener *net.TCPListener

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

	// Metrics
	openConnections  atomic.Int64
	receivedBytes    atomic.Int64
	receivedMessages atomic.Int64
}

func newTCPSource(cfg *tcpSourceConfig) *tcpSource {
	msgLenFieldParseLen := cfg.msgLenFieldLen
	switch msgLenFieldParseLen {
	case 3:
		msgLenFieldParseLen = 4
	case 5, 6, 7:
		msgLenFieldParseLen = 8
	}

	return &tcpSource{
		fanIn: pool.NewFanIn[*msg[*TCPMessage]](cfg.fanInBufferSize),

		wg: &sync.WaitGroup{},

		bufPool: sync.Pool{
			New: func() any {
				buf := make([]byte, tcpBufSize)
				return buf
			},
		},

		readTimeout: cfg.readTimeout,

		framingMode: cfg.framingMode,
		maxMsgSize:  cfg.maxMsgSize,

		headerLen:            cfg.headerLen,
		msgLenFieldOffset:    cfg.msgLenFieldOffset,
		msgLenFieldLen:       cfg.msgLenFieldLen,
		msgLenFieldParseLen:  msgLenFieldParseLen,
		msgLenFieldEndianess: cfg.msgLenFieldEndianess,

		delimiter:    cfg.delimiter,
		delimiterLen: len(cfg.delimiter),
	}
}

func (ts *tcpSource) setTelemetry(tel *internal.Telemetry) {
	ts.tel = tel
}

func (ts *tcpSource) init(ipAddr string, port uint16) error {
	parsedAddr, err := netip.ParseAddr(ipAddr)
	if err != nil {
		return err
	}

	addr := netip.AddrPortFrom(parsedAddr, port)
	listener, err := net.ListenTCP("tcp", net.TCPAddrFromAddrPort(addr))
	if err != nil {
		return err
	}

	ts.listener = listener

	ts.initMetrics()

	return nil
}

func (ts *tcpSource) initMetrics() {
	ts.tel.NewUpDownCounter("open_connections", func() int64 { return ts.openConnections.Load() })
	ts.tel.NewCounter("received_bytes", func() int64 { return ts.receivedBytes.Load() })
	ts.tel.NewCounter("received_messages", func() int64 { return ts.receivedMessages.Load() })
}

func (ts *tcpSource) run(ctx context.Context, outConnector msgConn[*TCPMessage]) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := ts.listener.Accept()
		if err != nil {
			// Check if the error is because the context is done
			select {
			case <-ctx.Done():
				return

			default:
				ts.tel.LogError("failed to accept connection", err)
				continue
			}
		}

		// Spawn a goroutine to handle the connection
		go ts.handleConn(ctx, conn, outConnector)
	}
}

func (ts *tcpSource) runBridge(ctx context.Context, outConnector msgConn[*TCPMessage]) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgOut, err := ts.fanIn.ReadTask()
		if err != nil {
			continue
		}

		if err := outConnector.Write(msgOut); err != nil {
			msgOut.Destroy()
			ts.tel.LogError("failed to write into output connector", err)
		}
	}
}

func (ts *tcpSource) handleConn(ctx context.Context, conn net.Conn, outConnector msgConn[*TCPMessage]) {
	ts.wg.Add(1)
	defer ts.wg.Done()

	defer conn.Close()

	go ts.runBridge(ctx, outConnector)

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
	ts.openConnections.Add(1)
	defer ts.openConnections.Add(-1)

	// Get the buffer from the pool
	buf := ts.bufPool.Get().([]byte)
	defer ts.bufPool.Put(buf)

	// Preallocate the accumulator
	accBaseCap := 4 * tcpBufSize
	acc := make([]byte, 0, accBaseCap)

	minAccLen := 0
	switch ts.framingMode {
	case TCPFramingModeDelimited:
		minAccLen = ts.delimiterLen
	case TCPFramingModeLengthPrefixed:
		minAccLen = ts.headerLen
	}

loop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set the read deadline
		conn.SetReadDeadline(time.Now().Add(ts.readTimeout))

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
			ts.tel.LogError("failed to read connection", err)
			return
		}

		// Append the new bytes to the accumulator
		acc = append(acc, buf[:n]...)

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
			switch ts.framingMode {
			case TCPFramingModeDelimited:
				msgLen = bytes.Index(acc, ts.delimiter)
				totLen = msgLen + ts.delimiterLen

			case TCPFramingModeLengthPrefixed:
				msgLen = ts.parseHeader(acc[:ts.headerLen])
				totLen = msgLen + ts.headerLen
			}

			if msgLen == -1 || accLen < totLen {
				// If the message length is not found or the accumulator is too small,
				// break the loop and continue reading the TCP stream
				break
			}

			// Extract the message
			msg := acc[:totLen]

			// Handle the message and send the result to the output connector
			outMsg := ts.handleMessage(ctx, msg)
			outMsg.GetEnvelope().RemoteAddr = conn.RemoteAddr().String()
			if err := ts.fanIn.AddTask(outMsg); err != nil {
				outMsg.Destroy()
				ts.tel.LogError("failed to write message to fan in connector", err)
			}

			// Remove the message from the accumulator
			acc = acc[totLen:]

			// Check if the accumulator should be reset
			if len(acc) == 0 && cap(acc) > accBaseCap {
				acc = make([]byte, 0, accBaseCap)
				break
			}
		}

		// Prevent accumulator from growing too large
		if len(acc) > ts.maxMsgSize {
			ts.tel.LogWarn("message too large, closing connection")
			return
		}
	}
}

func (ts *tcpSource) parseHeader(header []byte) int {
	if len(header) < ts.headerLen {
		return -1
	}

	msgLenField := header[ts.msgLenFieldOffset : ts.msgLenFieldOffset+ts.msgLenFieldLen]

	buf := msgLenField
	// Check if the message length field should be extended
	if ts.msgLenFieldLen != ts.msgLenFieldParseLen {
		buf = make([]byte, ts.msgLenFieldParseLen)

		switch ts.msgLenFieldEndianess {
		case LittleEndian:
			copy(buf, msgLenField)
		case BigEndian:
			copy(buf[ts.msgLenFieldParseLen-ts.msgLenFieldLen:], msgLenField)
		}
	}

	switch ts.msgLenFieldEndianess {
	case LittleEndian:
		return ts.parseLittleEndianMsgLen(buf)
	case BigEndian:
		return ts.parseBigEndianMsgLen(buf)
	}

	return 0
}

func (ts *tcpSource) parseLittleEndianMsgLen(buf []byte) int {
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

func (ts *tcpSource) parseBigEndianMsgLen(buf []byte) int {
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

func (ts *tcpSource) handleMessage(ctx context.Context, rawMsg []byte) *msg[*TCPMessage] {
	// Create the trace for the incoming message
	_, span := ts.tel.NewTrace(ctx, "receive TCP message")
	defer span.End()

	// Create the TCP message
	tcpMsg := newTCPMessage()

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
	ts.receivedBytes.Add(int64(msgSize))
	ts.receivedMessages.Add(1)

	return msg
}

func (ts *tcpSource) close() {
	ts.listener.Close()

	ts.wg.Wait()
	ts.fanIn.Close()
}

/////////////
//  STAGE  //
/////////////

// TCPStage is an ingress stage that reads TCP connections and extracts messages.
type TCPStage struct {
	*stage[*TCPMessage, *TCPConfig]

	source *tcpSource
}

// NewTCPStage returns a new TCP stage.
func NewTCPStage(outputConnector msgConn[*TCPMessage], cfg *TCPConfig) *TCPStage {
	source := newTCPSource(&tcpSourceConfig{
		fanInBufferSize:      cfg.OutputQueueSize,
		readTimeout:          cfg.ReadTimeout,
		framingMode:          cfg.FramingMode,
		maxMsgSize:           cfg.MaxMessageSize,
		delimiter:            cfg.Delimiter,
		headerLen:            cfg.HeaderLen,
		msgLenFieldOffset:    cfg.MessageLengthFieldOffset,
		msgLenFieldLen:       cfg.MessageLengthFieldLen,
		msgLenFieldEndianess: cfg.MessageLengthFieldEndianess,
	})

	return &TCPStage{
		stage: newStage("tcp", source, outputConnector, cfg),

		source: source,
	}
}

// Init initializes the stage.
func (ts *TCPStage) Init(ctx context.Context) error {
	if err := ts.source.init(ts.cfg.IPAddr, ts.cfg.Port); err != nil {
		return err
	}

	return ts.stage.Init(ctx)
}

// Close closes the stage.
func (ts *TCPStage) Close() {
	ts.source.close()
	ts.stage.Close()
}
