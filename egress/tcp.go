package egress

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/pool"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

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

////////////////////////
//  WORKER ARGUMENTS  //
////////////////////////

type tcpWorkerArgs struct {
	conn         *net.TCPConn
	writeTimeout time.Duration
}

func newTCPWorkerArgs(conn *net.TCPConn, writeTimeout time.Duration) *tcpWorkerArgs {
	return &tcpWorkerArgs{
		conn:         conn,
		writeTimeout: writeTimeout,
	}
}

//////////////////////
//  WORKER METRICS  //
//////////////////////

type tcpWorkerMetrics struct {
	once sync.Once

	deliveredBytes atomic.Int64
}

var tcpWorkerMetricsInst = &tcpWorkerMetrics{}

func (twm *tcpWorkerMetrics) init(tel *internal.Telemetry) {
	twm.once.Do(func() {
		twm.initMetrics(tel)
	})
}

func (twm *tcpWorkerMetrics) initMetrics(tel *internal.Telemetry) {
	tel.NewCounter("delivered_bytes", func() int64 { return twm.deliveredBytes.Load() })
}

func (twm *tcpWorkerMetrics) addDeliveredBytes(amount int) {
	twm.deliveredBytes.Add(int64(amount))
}

/////////////////////////////
//  WORKER IMPLEMENTATION  //
/////////////////////////////

type tcpWorker[T msgSer] struct {
	pool.BaseWorker

	conn         *net.TCPConn
	writeTimeout time.Duration

	metrics *tcpWorkerMetrics
}

func newTCPWorkerInstMaker[T msgSer]() workerInstanceMaker[*tcpWorkerArgs, T] {
	return func() workerInstance[*tcpWorkerArgs, T] {
		return &tcpWorker[T]{
			metrics: tcpWorkerMetricsInst,
		}
	}
}

func (tw *tcpWorker[T]) Init(_ context.Context, args *tcpWorkerArgs) error {
	tw.conn = args.conn
	tw.writeTimeout = args.writeTimeout

	tw.metrics.init(tw.Tel)

	return nil
}

func (tw *tcpWorker[T]) Deliver(ctx context.Context, msgIn *msg[T]) error {
	_, span := tw.Tel.NewTrace(ctx, "deliver TCP message")
	defer span.End()

	// Set the write timeout
	if err := tw.conn.SetWriteDeadline(time.Now().Add(tw.writeTimeout)); err != nil {
		return err
	}

	tcpMsg := msgIn.GetBody()

	tcpMsgRaw := tcpMsg.GetBytes()
	deliveredBytes, err := tw.conn.Write(tcpMsgRaw)
	if err != nil {
		return err
	}

	span.SetAttributes(attribute.Int("message_size", len(tcpMsgRaw)))

	// Update metrics
	tw.metrics.addDeliveredBytes(deliveredBytes)

	return nil
}

func (tw *tcpWorker[T]) Close(_ context.Context) error {
	return nil
}

/////////////
//  STAGE  //
/////////////

// TCPStage is an egress stage that writes messages to a TCP connection.
type TCPStage[T msgSer] struct {
	stage[*tcpWorkerArgs, T, *TCPConfig]

	conn *net.TCPConn
}

// NewTCPStage returns a new TCP egress stage.
func NewTCPStage[T msgSer](inputConnector msgConn[T], cfg *TCPConfig) *TCPStage[T] {
	return &TCPStage[T]{
		stage: newStageSingle("tcp", inputConnector, newTCPWorkerInstMaker[T](), cfg),
	}
}

// Init initializes the stage.
func (ts *TCPStage[T]) Init(ctx context.Context) error {
	cfg := ts.Config()

	// Parse the IP address
	parsedAddr, err := netip.ParseAddr(cfg.IPAddr)
	if err != nil {
		return err
	}
	addr := net.TCPAddrFromAddrPort(netip.AddrPortFrom(parsedAddr, cfg.Port))

	// Dial the TCP connection
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}

	ts.conn = conn

	return ts.stage.Init(ctx, newTCPWorkerArgs(ts.conn, cfg.WriteTimeout))
}
