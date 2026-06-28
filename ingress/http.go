package ingress

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/FerroO2000/goccia/ingress/metrics"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/future"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/link"
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
	DefaultHTTPConfigResponseTimeout    = 10 * time.Second
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
	ResponseTimeout    time.Duration
	WriteTimeout       time.Duration
	OutputQueueSize    int
	TLSEnabled         bool
	TLSConfig          *tls.Config
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
		ResponseTimeout:    DefaultHTTPConfigResponseTimeout,
		WriteTimeout:       DefaultHTTPConfigWriteTimeout,
		OutputQueueSize:    DefaultHTTPConfigOutputQueueSize,
		TLSEnabled:         false,
		TLSConfig:          nil,
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

	config.CheckGreaterThanZero(ac, "ResponseTimeout", &c.ResponseTimeout, DefaultHTTPConfigResponseTimeout)

	config.CheckGreaterThanZero(ac, "WriteTimeout", &c.WriteTimeout, DefaultHTTPConfigWriteTimeout)

	config.CheckGreaterThanZero(ac, "OutputQueueSize", &c.OutputQueueSize, DefaultHTTPConfigOutputQueueSize)
}

// ─── Message ────────────────────────────────────────────────────────────────|

type HTTPMessage = message.HTTPRequest

// ─── Environment ────────────────────────────────────────────────────────────|

type httpEnv struct {
	*env.BaseEnv[*HTTPConfig, *metrics.HttpStage]

	link   *link.HTTP
	server *http.Server

	responseTimeout    time.Duration
	maxRequestBodySize int64
	tlsEnabled         bool
}

func newHTTPEnv(link *link.HTTP, config *HTTPConfig) *httpEnv {
	return &httpEnv{
		BaseEnv: env.NewIngressEnv(config, metrics.NewHttpStage()),

		link: link,
	}
}

func (e *httpEnv) Init(ctx context.Context) error {
	if err := e.BaseEnv.Init(ctx); err != nil {
		return err
	}

	e.initServer()
	if err := e.initTLS(); err != nil {
		return err
	}

	e.responseTimeout = e.Config.ResponseTimeout
	e.maxRequestBodySize = int64(e.Config.MaxRequestBodySize)

	return nil
}

func (e *httpEnv) initTLS() error {
	if !e.Config.TLSEnabled {
		return nil
	}

	if e.Config.TLSConfig == nil {
		return errors.New("HTTP TLS is enabled but its configuration is missing")
	}

	serverTLSConfig := e.Config.TLSConfig.Clone()
	if serverTLSConfig.MinVersion == 0 {
		serverTLSConfig.MinVersion = tls.VersionTLS12
	}

	if len(serverTLSConfig.Certificates) == 0 && serverTLSConfig.GetCertificate == nil {
		return errors.New("HTTP TLS requires a certificate or GetCertificate callback")
	}

	e.server.TLSConfig = serverTLSConfig
	e.tlsEnabled = true

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

	err := r.listenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		r.env.Tel.LogError("HTTP server stopped", err)
		r.runServerDone <- struct{}{}
	}
}

func (r *httpRunner) listenAndServe() error {
	if r.env.tlsEnabled {
		return r.env.server.ListenAndServeTLS("", "")
	}

	return r.env.server.ListenAndServe()
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

func (r *httpRunner) readBody(resWriter http.ResponseWriter, bodyReader io.ReadCloser) ([]byte, bool) {
	reader := http.MaxBytesReader(resWriter, bodyReader, r.env.maxRequestBodySize)

	body, err := io.ReadAll(reader)
	if err != nil {
		http.Error(resWriter, "request body too large", http.StatusRequestEntityTooLarge)
		return nil, false
	}

	return body, true
}

func (r *httpRunner) makeRequestMessage(req *http.Request, body []byte) *msg[*HTTPMessage] {
	msgBody := &HTTPMessage{
		Method:     req.Method,
		Path:       req.URL.Path,
		Query:      req.URL.RawQuery,
		Header:     req.Header.Clone(),
		Body:       body,
		RemoteAddr: req.RemoteAddr,
	}

	return message.NewMessage(msgBody)
}

func (r *httpRunner) writeRequestMessage(
	resWriter http.ResponseWriter, correlationID uint64, reqMessage *msg[*HTTPMessage],
) bool {

	err := r.fanIn.Write(reqMessage)
	if err != nil {
		// If the shutdown sequence order is correct,
		// this error should never happen
		r.env.link.RejectFuture(correlationID, err)
		reqMessage.Destroy()
		http.Error(resWriter, "service unavailable", http.StatusServiceUnavailable)
		return false
	}

	return true
}

func (r *httpRunner) writeResponse(resWriter http.ResponseWriter, res *message.HTTPResponse) {
	// Write the headers
	for key, values := range res.Header {
		for _, value := range values {
			resWriter.Header().Add(key, value)
		}
	}

	resWriter.WriteHeader(res.StatusCode)
	resWriter.Write(res.Body)
}

func (r *httpRunner) handleRequest(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	ctx := req.Context()

	r.env.Metrics.IncrementRequests()

	body, ok := r.readBody(w, req.Body)
	if !ok {
		return
	}

	reqMessage := r.makeRequestMessage(req, body)

	// Set the receive time and the timestamp
	now := time.Now()
	reqMessage.SetReceiveTime(now)
	reqMessage.SetTimestamp(now)

	// Create the future
	correlationID, fut := r.env.link.NewFuture()
	reqMessage.SetCorrelationID(correlationID)

	// Send the message to the output (next stage)
	if ok := r.writeRequestMessage(w, correlationID, reqMessage); !ok {
		return
	}

	// Wait for the response using the future
	futureCtx, cancel := context.WithTimeout(ctx, r.env.responseTimeout)
	defer cancel()

	resMessage, state, err := fut.Await(futureCtx)

	switch state {
	case future.StateResolved:
		if resMessage == nil {
			http.Error(w, "bad gateway", http.StatusBadGateway)
			return
		}

		r.writeResponse(w, resMessage.GetBody())
		resMessage.Destroy()

		requestDuration := time.Since(now).Milliseconds()
		r.env.Metrics.RecordRequestDuration(ctx, int(requestDuration))

	case future.StateRejected:
		http.Error(w, "bad gateway", http.StatusBadGateway)

	case future.StateTimedOut:
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
		}

		if !r.env.link.DeleteFuture(correlationID) {
			// Resolve or reject won the shard lock before cancellation could
			// delete the future. Completion happens while that lock is held, so
			// Result is nonblocking here. Collect a resolved response to preserve
			// the ownership transfer from egress and release its message.
			lateResponse, lateState, _ := fut.Result()
			if lateState == future.StateResolved && lateResponse != nil {
				lateResponse.Destroy()
			}
		}
	}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

type HTTPStage struct {
	*stage.IngressStage[*HTTPMessage, *httpEnv]
}

func NewHTTPStage(link *link.HTTP, outConnector msgConn[*HTTPMessage], config *HTTPConfig) *HTTPStage {
	return &HTTPStage{
		IngressStage: stage.NewIngressStageFromRunner[*HTTPMessage](
			"http", newHTTPEnv(link, config), newHTTPRunner(outConnector),
		),
	}
}
