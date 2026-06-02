package ingress

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/FerroO2000/goccia/ingress/metrics"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default values for the UDP stage configuration.
const (
	DefaultUDPConfigIPAddr            = "0.0.0.0"
	DefaultUDPConfigPort       uint16 = 20_000
	DefaultUDPConfigBufferSize uint16 = 1472
)

// UDPConfig structs contains the configuration for the UDP stage.
type UDPConfig struct {
	// IPAddr is the IP address to listen on.
	IPAddr string

	// Port is the port to listen on.
	Port uint16

	// BufferSize is the size of the buffer used to receive messages.
	// It will also set the default dimension of the Payload field
	// of the UDP message.
	BufferSize uint16
}

// NewUDPConfig returns the default configuration for the UDP stage.
func NewUDPConfig() *UDPConfig {
	return &UDPConfig{
		IPAddr:     DefaultUDPConfigIPAddr,
		Port:       DefaultUDPConfigPort,
		BufferSize: DefaultUDPConfigBufferSize,
	}
}

// Validate checks the configuration.
func (c *UDPConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotEmpty(ac, "IPAddr", &c.IPAddr, DefaultUDPConfigIPAddr)

	config.CheckNotZero(ac, "Port", &c.Port, DefaultUDPConfigPort)

	config.CheckNotZero(ac, "BufferSize", &c.BufferSize, DefaultUDPConfigBufferSize)
}

// ─── Message ────────────────────────────────────────────────────────────────|

var _ msgSer = (*UDPMessage)(nil)

// UDPMessage represents a UDP message.
type UDPMessage struct {
	// Payload of the UDP datagram.
	Payload []byte
	// PayloadSize is the number of bytes of the payload.
	PayloadSize int

	pool *udpMessagePool
}

// NewUDPMessage returns a new UDP message, without using the message pool.
func NewUDPMessage(payloadSize int) *UDPMessage {
	return &UDPMessage{
		Payload:     make([]byte, payloadSize),
		PayloadSize: 0,

		pool: nil,
	}
}

// Destroy cleans up the message.
func (um *UDPMessage) Destroy() {
	if um.pool != nil {
		um.pool.putMessage(um)
	}
}

// GetBytes returns the bytes of the UDP payload.
func (um *UDPMessage) GetBytes() []byte {
	return um.Payload[:um.PayloadSize]
}

// ─── Message Pool ───────────────────────────────────────────────────────────|

type udpMessagePool struct {
	pool        sync.Pool
	payloadSize int
}

func newUDPMessagePool(payloadSize int) *udpMessagePool {
	ump := &udpMessagePool{
		payloadSize: payloadSize,
	}

	ump.pool.New = func() any {
		return &UDPMessage{
			Payload:     make([]byte, payloadSize),
			PayloadSize: 0,

			pool: ump,
		}
	}

	return ump
}

func (ump *udpMessagePool) getMessage() *UDPMessage {
	msg := ump.pool.Get().(*UDPMessage)
	msg.Payload = msg.Payload[:ump.payloadSize]
	return msg
}

func (ump *udpMessagePool) putMessage(um *UDPMessage) {
	um.PayloadSize = 0
	ump.pool.Put(um)
}

// ─── Environment ────────────────────────────────────────────────────────────|

type udpEnv struct {
	*env.BaseEnv[*UDPConfig, *metrics.UdpStage]

	messagePool *udpMessagePool

	conn *net.UDPConn
}

func newUDPEnv(config *UDPConfig) *udpEnv {
	return &udpEnv{
		BaseEnv: env.NewIngressEnv(config, metrics.NewUdpStage()),
	}
}

func (ue *udpEnv) Init(ctx context.Context) error {
	if err := ue.BaseEnv.Init(ctx); err != nil {
		return err
	}

	// Create the message pool
	ue.messagePool = newUDPMessagePool(int(ue.Config.BufferSize))

	// Parse the IP address
	parsedAddr, err := netip.ParseAddr(ue.Config.IPAddr)
	if err != nil {
		return err
	}
	addr := net.UDPAddrFromAddrPort(netip.AddrPortFrom(parsedAddr, ue.Config.Port))

	// Listen on the specified address
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	ue.conn = conn

	return nil
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*udpEnv] = (*udpRunner)(nil)

type udpRunner struct {
	*runnerBase[*udpEnv, *UDPMessage]
}

func newUDPRunner(outConnector msgConn[*UDPMessage]) *udpRunner {
	return &udpRunner{
		runnerBase: newRunnerBase[*udpEnv](outConnector),
	}
}

func (ur *udpRunner) Run(ctx context.Context) {
	defer ur.notifyRunDone()

	done := make(chan struct{})
	defer close(done)

	// Hacky method to close the connection when the context is done
	go func() {
		select {
		case <-ctx.Done():
			ur.env.conn.Close()
		case <-done:
		}
	}()

	buf := make([]byte, ur.env.Config.BufferSize)

	for {
		// Read the UDP payload
		n, err := ur.env.conn.Read(buf)
		if err != nil {
			// Check if the connection is closed by the context
			if errors.Is(err, net.ErrClosed) && ctx.Err() != nil {
				return
			}

			ur.env.Tel.LogError("failed to read connection", err)
			return
		}

		// Handle the buffer and send the message
		msgOut := ur.handleBuf(ctx, buf[:n])
		if err := ur.outConnector.Write(msgOut); err != nil {
			msgOut.Destroy()
			ur.env.Tel.LogError("failed to write message to output connector", err)
		}
	}
}

func (ur *udpRunner) handleBuf(ctx context.Context, buf []byte) *msg[*UDPMessage] {
	ur.env.Tel.LogDebug("received UDP datagram")

	// Create the trace for the incoming datagram
	_, span := ur.env.Tel.StartTrace(ctx, "receive UDP datagram")
	defer span.End()

	// Create the UDP message
	udpMsg := ur.env.messagePool.getMessage()

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
	ur.env.Metrics.AddReceivedBytes(uint(payloadSize))
	ur.env.Metrics.IncrementReceivedMessages()

	return msg
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// UDPStage is an ingress stage that reads UDP datagrams.
type UDPStage struct {
	*stage.IngressStage[*UDPMessage, *udpEnv]
}

// NewUDPStage returns a new UDP stage.
func NewUDPStage(outConnector msgConn[*UDPMessage], cfg *UDPConfig) *UDPStage {
	return &UDPStage{
		IngressStage: stage.NewIngressStageFromRunner[*UDPMessage](
			"udp", newUDPEnv(cfg), newUDPRunner(outConnector),
		),
	}
}
