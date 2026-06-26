package ingress

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default values for the HTTP ingress stage configuration.
const (
	DefaultHTTPConfigIPAddr             = "0.0.0.0"
	DefaultHTTPConfigPort               = 8080
	DefaultHTTPConfigReadTimeout        = 10 * time.Second
	DefaultHTTPConfigReadHeaderTimeout  = 5 * time.Second
	DefaultHTTPConfigShutdownTimeout    = 10 * time.Second
	DefaultHTTPConfigIdleTimeout        = 60 * time.Second
	DefaultHTTPConfigMaxRequestBodySize = 4 << 20 // 4 MiB
	DefaultHTTPConfigWriteTimeout       = 10 * time.Second
	DefaultHTTPConfigOutputQueueSize    = 512
)

type HTTPConfig struct {
	IPAddr             string
	Port               uint16
	ReadTimeout        time.Duration
	ReadHeaderTimeout  time.Duration
	ShutdownTimeout    time.Duration
	IdleTimeout        time.Duration
	MaxRequestBodySize int
	WriteTimeout       time.Duration
	OutputQueueSize    int
}

func NewHTTPConfig() *HTTPConfig {
	return &HTTPConfig{
		IPAddr:             DefaultHTTPConfigIPAddr,
		Port:               DefaultHTTPConfigPort,
		ReadTimeout:        DefaultHTTPConfigReadTimeout,
		ReadHeaderTimeout:  DefaultHTTPConfigReadHeaderTimeout,
		ShutdownTimeout:    DefaultHTTPConfigShutdownTimeout,
		IdleTimeout:        DefaultHTTPConfigIdleTimeout,
		MaxRequestBodySize: DefaultHTTPConfigMaxRequestBodySize,
		WriteTimeout:       DefaultHTTPConfigWriteTimeout,
		OutputQueueSize:    DefaultHTTPConfigOutputQueueSize,
	}
}

// Validate checks the configuration.
func (c *HTTPConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotEmpty(ac, "IPAddr", &c.IPAddr, DefaultHTTPConfigIPAddr)
	config.CheckNotZero(ac, "Port", &c.Port, DefaultHTTPConfigPort)

	config.CheckGreaterThanZero(ac, "ReadTimeout", &c.ReadTimeout, DefaultHTTPConfigReadTimeout)

	config.CheckGreaterThanZero(ac, "ReadHeaderTimeout", &c.ReadHeaderTimeout, DefaultHTTPConfigReadHeaderTimeout)

	config.CheckGreaterThanZero(ac, "ShutdownTimeout", &c.ShutdownTimeout, DefaultHTTPConfigShutdownTimeout)

	config.CheckGreaterThanZero(ac, "IdleTimeout", &c.IdleTimeout, DefaultHTTPConfigIdleTimeout)

	config.CheckGreaterThanZero(ac, "MaxRequestBodySize", &c.MaxRequestBodySize, DefaultHTTPConfigMaxRequestBodySize)

	config.CheckGreaterThanZero(ac, "WriteTimeout", &c.WriteTimeout, DefaultHTTPConfigWriteTimeout)

	config.CheckGreaterThanZero(ac, "OutputQueueSize", &c.OutputQueueSize, DefaultHTTPConfigOutputQueueSize)
}

// ─── Message ────────────────────────────────────────────────────────────────|

type HTTPMessage struct {
	Method     string
	Path       string
	Query      string
	Header     http.Header
	Body       []byte
	RemoteAddr string
}

func (m *HTTPMessage) Destroy() {
	m.Method = ""
	m.Path = ""
	m.Query = ""
	m.Header = nil
	m.Body = nil
	m.RemoteAddr = ""
}

// ─── Environment ────────────────────────────────────────────────────────────|

type httpEnv struct {
	*env.BaseEnv[*HTTPConfig, *metrics.EmptyMetrics]

	server *http.Server

	maxRequestBodySize int64
}

func newHTTPEnv(config *HTTPConfig) *httpEnv {
	return &httpEnv{
		BaseEnv: env.NewIngressEnv(config, metrics.NewEmptyMetrics()),
	}
}

func (e *httpEnv) Init(ctx context.Context) error {
	if err := e.BaseEnv.Init(ctx); err != nil {
		return err
	}

	e.initServer()

	e.maxRequestBodySize = int64(e.Config.MaxRequestBodySize)

	return nil
}

func (e *httpEnv) initServer() {
	e.server = &http.Server{
		Addr:              net.JoinHostPort(e.Config.IPAddr, strconv.Itoa(int(e.Config.Port))),
		ReadTimeout:       e.Config.ReadTimeout,
		ReadHeaderTimeout: e.Config.ReadHeaderTimeout,
		WriteTimeout:      e.Config.WriteTimeout,
		IdleTimeout:       e.Config.IdleTimeout,
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*httpEnv] = (*httpRunner)(nil)

type httpRunner struct {
	*runnerFanInBase[*httpEnv, *HTTPMessage]

	runServerDone chan struct{}
}

func newHTTPRunner(outConnector msgConn[*HTTPMessage]) *httpRunner {
	return &httpRunner{
		runnerFanInBase: newRunnerFanInBase[*httpEnv](outConnector),

		runServerDone: make(chan struct{}, 1),
	}
}

func (r *httpRunner) Init(_ context.Context) error {
	r.initFanIn(uint64(r.env.Config.OutputQueueSize))
	return nil
}

func (r *httpRunner) runServer() {
	defer close(r.runServerDone)

	r.env.server.Handler = http.HandlerFunc(r.handleRequest)

	err := r.env.server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		r.env.Tel.LogError("HTTP server stopped", err)
		r.runServerDone <- struct{}{}
	}
}

func (r *httpRunner) shutdownServer() {
	timeout := r.env.Config.ShutdownTimeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := r.env.server.Shutdown(shutdownCtx); err != nil {
		r.env.Tel.LogError("failed to gracefully shut down HTTP server", err)

		if err := r.env.server.Close(); err != nil {
			r.env.Tel.LogError("failed to close HTTP server", err)
		}
	}

	<-r.runServerDone
}

func (r *httpRunner) Run(ctx context.Context) {
	defer r.drainAndNotifyRunDone()

	go r.runOutputBridge(context.WithoutCancel(ctx))

	go r.runServer()

	select {
	case <-ctx.Done():
		r.shutdownServer()

	case <-r.runServerDone:
	}
}

func (r *httpRunner) handleRequest(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	bodyReader := http.MaxBytesReader(w, req.Body, r.env.maxRequestBodySize)
	body, err := io.ReadAll(bodyReader)
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}

	msgBody := &HTTPMessage{
		Method:     req.Method,
		Path:       req.URL.Path,
		Query:      req.URL.RawQuery,
		Header:     req.Header.Clone(),
		Body:       body,
		RemoteAddr: req.RemoteAddr,
	}

	msgOut := message.NewMessage(msgBody)
	now := time.Now()
	msgOut.SetReceiveTime(now)
	msgOut.SetTimestamp(now)

	if err := r.fanIn.Write(msgOut); err != nil {
		msgOut.Destroy()
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	// TODO! wait the future

	w.WriteHeader(http.StatusAccepted)
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

type HTTPStage struct {
	*stage.IngressStage[*HTTPMessage, *httpEnv]
}

func NewHTTPStage(outConnector msgConn[*HTTPMessage], config *HTTPConfig) *HTTPStage {
	return &HTTPStage{
		IngressStage: stage.NewIngressStageFromRunner[*HTTPMessage](
			"http", newHTTPEnv(config), newHTTPRunner(outConnector),
		),
	}
}
