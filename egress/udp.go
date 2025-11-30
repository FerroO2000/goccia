package egress

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/pool"
	stageCommon "github.com/FerroO2000/goccia/internal/stage"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

// UDPConfig structs contains the configuration for the UDP egress stage.
type UDPConfig struct {
	Stage *stageCommon.Config

	// IPAddr is the destination IP address.
	//
	// Default: 127.0.0.1
	IPAddr string

	// Port is the destination port.
	//
	// Default: 20_000
	Port uint16
}

// DefaultUDPConfig returns the default configuration for the UDP egress stage.
func DefaultUDPConfig(runningMode stageCommon.RunningMode) *UDPConfig {
	return &UDPConfig{
		Stage: stageCommon.DefaultConfig(runningMode),

		IPAddr: "127.0.0.1",
		Port:   20_000,
	}
}

////////////////////////
//  WORKER ARGUMENTS  //
////////////////////////

type udpWorkerArgs struct {
	conn *net.UDPConn
}

func newUDPWorkerArgs(conn *net.UDPConn) *udpWorkerArgs {
	return &udpWorkerArgs{
		conn: conn,
	}
}

//////////////////////
//  WORKER METRICS  //
//////////////////////

type udpWorkerMetrics struct {
	once sync.Once

	deliveredBytes atomic.Int64
}

var udpWorkerMetricsInst = &udpWorkerMetrics{}

func (uwm *udpWorkerMetrics) init(tel *internal.Telemetry) {
	uwm.once.Do(func() {
		uwm.initMetrics(tel)
	})
}

func (uwm *udpWorkerMetrics) initMetrics(tel *internal.Telemetry) {
	tel.NewCounter("delivered_bytes", func() int64 { return uwm.deliveredBytes.Load() })
}

func (uwm *udpWorkerMetrics) addDeliveredBytes(amount int) {
	uwm.deliveredBytes.Add(int64(amount))
}

/////////////////////////////
//  WORKER IMPLEMENTATION  //
/////////////////////////////

type udpWorker[T msgSer] struct {
	pool.BaseWorker

	conn *net.UDPConn

	metrics *udpWorkerMetrics
}

func newUDPWorkerInstMaker[T msgSer]() workerInstanceMaker[*udpWorkerArgs, T] {
	return func() workerInstance[*udpWorkerArgs, T] {
		return &udpWorker[T]{
			metrics: udpWorkerMetricsInst,
		}
	}
}

func (uw *udpWorker[T]) Init(_ context.Context, args *udpWorkerArgs) error {
	uw.conn = args.conn

	uw.metrics.init(uw.Tel)

	return nil
}

func (uw *udpWorker[T]) Deliver(ctx context.Context, msgIn *msg[T]) error {
	_, span := uw.Tel.NewTrace(ctx, "deliver UDP message")
	defer span.End()

	udpMsg := msgIn.GetEnvelope()

	payload := udpMsg.GetBytes()
	payloadSize := len(payload)

	deliveredBytes, err := uw.conn.Write(payload)
	if err != nil {
		return err
	}

	span.SetAttributes(attribute.Int("payload_size", payloadSize))

	// Update metrics
	uw.metrics.addDeliveredBytes(deliveredBytes)

	return nil
}

func (uw *udpWorker[T]) Close(_ context.Context) error {
	return nil
}

/////////////
//  STAGE  //
/////////////

// UDPStage is an egress stage that sends UDP datagrams.
type UDPStage[T msgSer] struct {
	stage[*udpWorkerArgs, T]

	cfg *UDPConfig

	conn *net.UDPConn
}

// NewUDPStage returns a new UDP egress stage.
func NewUDPStage[T msgSer](inputConnector msgConn[T], cfg *UDPConfig) *UDPStage[T] {
	return &UDPStage[T]{
		stage: newStage(
			"udp", inputConnector, newUDPWorkerInstMaker[T](), cfg.Stage,
		),

		cfg: cfg,
	}
}

// Init initializes the stage.
func (us *UDPStage[T]) Init(ctx context.Context) error {
	// Parse the IP address
	parsedAddr, err := netip.ParseAddr(us.cfg.IPAddr)
	if err != nil {
		return err
	}
	addr := net.UDPAddrFromAddrPort(netip.AddrPortFrom(parsedAddr, us.cfg.Port))

	// Dial the UDP connection
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	us.conn = conn

	return us.stage.Init(ctx, newUDPWorkerArgs(conn))
}
