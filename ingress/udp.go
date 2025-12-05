package ingress

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"go.opentelemetry.io/otel/attribute"
)

const (
	udpPayloadSize = 1474
)

//////////////
//  CONFIG  //
//////////////

// Default values for the UDP stage configuration.
const (
	DefaultUDPConfigIPAddr = "0.0.0.0"
	DefaultUDPConfigPort   = 20_000
)

// UDPConfig structs contains the configuration for the UDP stage.
type UDPConfig struct {
	// IPAddr is the IP address to listen on.
	IPAddr string

	// Port is the port to listen on.
	Port uint16
}

// NewUDPConfig returns the default configuration for the UDP stage.
func NewUDPConfig() *UDPConfig {
	return &UDPConfig{
		IPAddr: DefaultUDPConfigIPAddr,
		Port:   DefaultUDPConfigPort,
	}
}

// Validate checks the configuration.
func (c *UDPConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotEmpty(ac, "IPAddr", &c.IPAddr, DefaultUDPConfigIPAddr)
}

///////////////
//  MESSAGE  //
///////////////

var _ msgSer = (*UDPMessage)(nil)

var udpMessagePool = sync.Pool{
	New: func() any {
		return &UDPMessage{
			Payload: make([]byte, udpPayloadSize),
		}
	},
}

// UDPMessage represents a UDP message.
type UDPMessage struct {
	// Payload of the UDP datagram.
	Payload []byte
	// PayloadSize is the number of bytes of the payload.
	PayloadSize int
}

func newUDPMessage() *UDPMessage {
	return udpMessagePool.Get().(*UDPMessage)
}

// Destroy cleans up the message.
func (um *UDPMessage) Destroy() {
	udpMessagePool.Put(um)
}

// GetBytes returns the bytes of the UDP payload.
func (um *UDPMessage) GetBytes() []byte {
	return um.Payload
}

//////////////
//  SOURCE  //
//////////////

var _ source[*UDPMessage] = (*udpSource)(nil)

type udpSource struct {
	tel *internal.Telemetry

	conn *net.UDPConn

	// Metrics
	receivedMessages atomic.Int64
	receivedBytes    atomic.Int64
}

func newUDPSource() *udpSource {
	return &udpSource{}
}

func (us *udpSource) setTelemetry(tel *internal.Telemetry) {
	us.tel = tel
}

func (us *udpSource) init(ipAddr string, port uint16) error {
	parsedAddr, err := netip.ParseAddr(ipAddr)
	if err != nil {
		return err
	}

	addr := net.UDPAddrFromAddrPort(netip.AddrPortFrom(parsedAddr, port))
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	us.conn = conn

	us.initMetrics()

	return nil
}

func (us *udpSource) initMetrics() {
	us.tel.NewCounter("received_messages", func() int64 { return us.receivedMessages.Load() })
	us.tel.NewCounter("received_bytes", func() int64 { return us.receivedBytes.Load() })
}

func (us *udpSource) run(ctx context.Context, outConnector msgConn[*UDPMessage]) {
	// Hacky method to close the connection when the context is done
	go func() {
		<-ctx.Done()
		us.conn.Close()
	}()

	buf := make([]byte, udpPayloadSize)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// read the UDP payload
		_, err := us.conn.Read(buf)
		if err != nil {
			// Check if the connection is closed
			if errors.Is(err, net.ErrClosed) {
				select {
				case <-ctx.Done():
					return
				default:
				}
			}

			us.tel.LogError("failed to read connection", err)
			return
		}

		// Handle the buffer and send the message
		msgOut := us.handleBuf(ctx, buf)
		if err := outConnector.Write(msgOut); err != nil {
			msgOut.Destroy()
			us.tel.LogError("failed to write message to output connector", err)
		}
	}
}

func (us *udpSource) handleBuf(ctx context.Context, buf []byte) *msg[*UDPMessage] {
	// Create the trace for the incoming datagram
	_, span := us.tel.NewTrace(ctx, "receive UDP datagram")
	defer span.End()

	// Create the UDP message
	udpMsg := newUDPMessage()

	// Extract the payload from the buffer
	payloadSize := len(buf)
	udpMsg.PayloadSize = payloadSize
	copy(udpMsg.Payload, buf)

	msg := message.NewMessage(udpMsg)

	// Set the receive time and the timestamp
	recvTime := time.Now()
	msg.SetReceiveTime(recvTime)
	msg.SetTimestamp(recvTime)

	// Save the span into the message
	span.SetAttributes(attribute.Int("payload_size", payloadSize))
	msg.SaveSpan(span)

	// Update metrics
	us.receivedBytes.Add(int64(payloadSize))
	us.receivedMessages.Add(1)

	return msg
}

/////////////
//  STAGE  //
/////////////

// UDPStage is an ingress stage that reads UDP datagrams.
type UDPStage struct {
	*stage[*UDPMessage, *UDPConfig]

	source *udpSource
}

// NewUDPStage returns a new UDP stage.
func NewUDPStage(outputConnector msgConn[*UDPMessage], cfg *UDPConfig) *UDPStage {
	source := newUDPSource()

	return &UDPStage{
		stage: newStage("udp", source, outputConnector, cfg),

		source: source,
	}
}

// Init initializes the stage.
func (us *UDPStage) Init(ctx context.Context) error {
	if err := us.source.init(us.cfg.IPAddr, us.cfg.Port); err != nil {
		return err
	}

	return us.stage.Init(ctx)
}
