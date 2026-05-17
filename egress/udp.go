package egress

import (
	"context"
	"net"
	"net/netip"

	"github.com/FerroO2000/goccia/egress/metrics"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default values for the UDP egress stage configuration.
const (
	DefaultUDPConfigIPAddr = "127.0.0.1"
	DefaultUDPConfigPort   = 20_000
)

// UDPConfig structs contains the configuration for the UDP egress stage.
type UDPConfig struct {
	*config.Base

	// IPAddr is the destination IP address.
	IPAddr string

	// Port is the destination port.
	Port uint16
}

// NewUDPConfig returns the default configuration for the UDP egress stage.
func NewUDPConfig(runningMode config.StageRunningMode) *UDPConfig {
	return &UDPConfig{
		Base: config.NewBase(runningMode),

		IPAddr: "127.0.0.1",
		Port:   20_000,
	}
}

// Validate checks the configuration.
func (c *UDPConfig) Validate(ac *config.AnomalyCollector) {
	c.Base.Validate(ac)

	config.CheckNotEmpty(ac, "IPAddr", &c.IPAddr, DefaultUDPConfigIPAddr)
}

// ─── Environment ────────────────────────────────────────────────────────────|

type udpEnv struct {
	*env.BaseEnv[*UDPConfig, *metrics.UdpStage]

	conn *net.UDPConn
}

func newUDPEnv[T msgSer](config *UDPConfig) *udpEnv {
	return &udpEnv{
		BaseEnv: env.NewEgressEnv(config, metrics.NewUdpStage()),

		conn: nil,
	}
}

func (ue *udpEnv) Init(ctx context.Context) error {
	// Parse the IP address
	parsedAddr, err := netip.ParseAddr(ue.Config.IPAddr)
	if err != nil {
		return err
	}
	addr := net.UDPAddrFromAddrPort(netip.AddrPortFrom(parsedAddr, ue.Config.Port))

	// Dial the UDP connection
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}
	ue.conn = conn

	return ue.BaseEnv.Init(ctx)
}

func (ue *udpEnv) Close(ctx context.Context) {
	if err := ue.conn.Close(); err != nil {
		ue.BaseEnv.Tel.LogError("failed to close connection", err)
	}

	ue.BaseEnv.Close(ctx)
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type udpWorker[T msgSer] struct {
	worker.BaseWorker[*udpEnv]
}

func newUDPWorkerMaker[T msgSer]() func() *udpWorker[T] {
	return func() *udpWorker[T] {
		return &udpWorker[T]{}
	}
}

func (uw *udpWorker[T]) Deliver(ctx context.Context, msgIn *msg[T]) error {
	_, span := uw.Tel.StartTrace(ctx, "deliver UDP message")
	defer span.End()

	udpMsg := msgIn.GetBody()

	payload := udpMsg.GetBytes()
	payloadSize := len(payload)

	deliveredBytes, err := uw.Env.conn.Write(payload)
	if err != nil {
		return err
	}

	span.SetAttributes(attribute.Int("payload_size", payloadSize))

	// Update metrics
	uw.Env.Metrics.AddDeliveredBytes(uint(deliveredBytes))

	return nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// UDPStage is an egress stage that sends UDP datagrams.
type UDPStage[T msgSer] struct {
	*stage.EgressStage[T, *udpEnv]
}

// NewUDPStage returns a new UDP egress stage.
func NewUDPStage[T msgSer](inputConnector msgConn[T], cfg *UDPConfig) *UDPStage[T] {
	env := newUDPEnv[T](cfg)

	return &UDPStage[T]{
		EgressStage: stage.NewEgressStage(
			"udp", inputConnector, env, newUDPWorkerMaker[T](), cfg.Stage,
		),
	}
}
