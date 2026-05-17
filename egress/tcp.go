package egress

import (
	"context"
	"net"
	"net/netip"
	"time"

	"github.com/FerroO2000/goccia/egress/metrics"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default values for the TCP egress stage configuration.
const (
	DefaultTCPConfigIPAddr       = "127.0.0.1"
	DefaultTCPConfigPort         = 20_000
	DefaultTCPConfigWriteTimeout = 10 * time.Second
)

// TCPConfig structs contains the configuration for the TCP egress stage.
type TCPConfig struct {
	// IPAddr is the destination IP address.
	IPAddr string

	// Port is the destination port.
	Port uint16

	// WriteTimeout is the timeout for writing messages to the TCP connection.
	WriteTimeout time.Duration
}

// NewTCPConfig returns a default TCPConfig.
func NewTCPConfig() *TCPConfig {
	return &TCPConfig{
		IPAddr:       DefaultTCPConfigIPAddr,
		Port:         DefaultTCPConfigPort,
		WriteTimeout: DefaultTCPConfigWriteTimeout,
	}
}

// Validate checks the configuration.
func (c *TCPConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotEmpty(ac, "IPAddr", &c.IPAddr, DefaultTCPConfigIPAddr)

	config.CheckNotNegative(ac, "WriteTimeout", &c.WriteTimeout, DefaultTCPConfigWriteTimeout)
	config.CheckNotZero(ac, "WriteTimeout", &c.WriteTimeout, DefaultTCPConfigWriteTimeout)
}

// ─── Environment ────────────────────────────────────────────────────────────|

type tcpEnv struct {
	*env.BaseEnv[*TCPConfig, *metrics.TcpStage]

	conn *net.TCPConn
}

func newTCPEnv(config *TCPConfig) *tcpEnv {
	return &tcpEnv{
		BaseEnv: env.NewEgressEnv(config, metrics.NewTcpStage()),

		conn: nil,
	}
}

func (te *tcpEnv) Init(ctx context.Context) error {
	// Parse the IP address
	parsedAddr, err := netip.ParseAddr(te.Config.IPAddr)
	if err != nil {
		return err
	}
	addr := net.TCPAddrFromAddrPort(netip.AddrPortFrom(parsedAddr, te.Config.Port))

	// Dial the TCP connection
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	te.conn = conn

	return te.BaseEnv.Init(ctx)
}

func (te *tcpEnv) Close(ctx context.Context) {
	if err := te.conn.Close(); err != nil {
		te.BaseEnv.Tel.LogError("failed to close connection", err)
	}

	te.BaseEnv.Close(ctx)
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type tcpWorker[T msgSer] struct {
	worker.BaseWorker[*tcpEnv]
}

func newTCPWorkerMaker[T msgSer]() func() *tcpWorker[T] {
	return func() *tcpWorker[T] {
		return &tcpWorker[T]{}
	}
}

func (tw *tcpWorker[T]) Deliver(ctx context.Context, msgIn *msg[T]) error {
	_, span := tw.Tel.StartTrace(ctx, "deliver TCP message")
	defer span.End()

	// Set the write timeout
	deadline := time.Now().Add(tw.Env.Config.WriteTimeout)
	if err := tw.Env.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}

	tcpMsg := msgIn.GetBody()

	tcpMsgRaw := tcpMsg.GetBytes()
	deliveredBytes, err := tw.Env.conn.Write(tcpMsgRaw)
	if err != nil {
		return err
	}

	span.SetAttributes(attribute.Int("message_size", len(tcpMsgRaw)))

	// Update metrics
	tw.Env.Metrics.AddDeliveredBytes(uint(deliveredBytes))

	return nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// TCPStage is an egress stage that writes messages to a TCP connection.
type TCPStage[T msgSer] struct {
	*stage.EgressStage[T, *tcpEnv]
}

// NewTCPStage returns a new TCP egress stage.
func NewTCPStage[T msgSer](inputConnector msgConn[T], cfg *TCPConfig) *TCPStage[T] {
	env := newTCPEnv(cfg)

	return &TCPStage[T]{
		EgressStage: stage.NewEgressStage(
			"tcp", inputConnector, env, newTCPWorkerMaker[T](), &config.Stage{
				RunningMode: config.StageRunningModeSingle,
			},
		),
	}
}
