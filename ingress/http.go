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
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
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

// HTTPConfig contains the configuration for the HTTP ingress stage.
type HTTPConfig struct {
	// IPAddr is the local IP address on which the HTTP server listens.
	IPAddr string

	// Port is the local TCP port on which the HTTP server listens.
	Port uint16

	// ReadTimeout is the maximum duration for reading an entire request,
	// including its body.
	ReadTimeout time.Duration

	// ReadHeaderTimeout is the maximum duration for reading request headers.
	ReadHeaderTimeout time.Duration

	// ShutdownTimeout is the grace period for shutting down the HTTP server.
	ShutdownTimeout time.Duration

	// IdleTimeout is the maximum time to wait for the next request on a
	// keep-alive connection.
	IdleTimeout time.Duration

	// MaxRequestBodySize is the maximum request body size in bytes.
	// Request bodies are read completely into memory.
	MaxRequestBodySize int

	// ResponseTimeout is the maximum duration to wait for a correlated
	// downstream response after the request enters the internal queue.
	ResponseTimeout time.Duration

	// WriteTimeout is the maximum duration for writing an HTTP response.
	WriteTimeout time.Duration

	// OutputQueueSize is the capacity of the internal queue between concurrent
	// HTTP handlers and the output connector.
	OutputQueueSize int

	// TLSEnabled controls whether the server accepts HTTPS connections.
	TLSEnabled bool

	// TLSConfig configures HTTPS. When TLS is enabled, it must provide at least
	// one certificate or a GetCertificate callback. The stage clones the value
	// during initialization and defaults a zero MinVersion to TLS 1.2.
	TLSConfig *tls.Config
}

// NewHTTPConfig returns the default configuration for the HTTP ingress stage.
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

// HTTPMessage is the request message emitted by the HTTP ingress stage.
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

// ─── Response Writer ────────────────────────────────────────────────────────|

type httpResponseWriter struct {
	http.ResponseWriter
	statusCode      int
	timestamp       time.Time
	requestBodySize int64
	bytesWritten    int64
	writeErr        error
}

func (r *httpResponseWriter) WriteHeader(statusCode int) {
	if r.statusCode != 0 {
		return
	}

	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *httpResponseWriter) Write(b []byte) (int, error) {
	if r.statusCode == 0 {
		r.statusCode = http.StatusOK
	}

	n, err := r.ResponseWriter.Write(b)
	r.bytesWritten += int64(n)

	if err != nil && r.writeErr == nil {
		r.writeErr = err
	}

	return n, err
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

	r.env.server.Handler = http.HandlerFunc(r.handle)

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

func (r *httpRunner) readBody(
	resWriter http.ResponseWriter, bodyReader io.ReadCloser,
) ([]byte, int64, bool) {
	limitedReader := http.MaxBytesReader(resWriter, bodyReader, r.env.maxRequestBodySize)

	body, err := io.ReadAll(limitedReader)
	bytesRead := int64(len(body))

	if err != nil {
		http.Error(resWriter, "request body too large", http.StatusRequestEntityTooLarge)
		return nil, bytesRead, false
	}

	return body, bytesRead, true
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
	ctx context.Context, resWriter http.ResponseWriter, correlationID uint64, reqMessage *msg[*HTTPMessage],
) bool {
	queueWriteTime := time.Now()

	ok := true
	outcome := "enqueued"

	err := r.fanIn.Write(reqMessage)
	if err != nil {
		// If the shutdown sequence order is correct,
		// this error should never happen
		r.env.link.RejectFuture(correlationID, err)
		reqMessage.Destroy()
		http.Error(resWriter, "service unavailable", http.StatusServiceUnavailable)

		ok = false
		outcome = "rejected"
	}

	r.env.Metrics.RecordGocciaHttpIngressQueueWaitDuration(
		ctx, time.Since(queueWriteTime).Seconds(),
		outcome,
	)

	return ok
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

func (*httpRunner) getURLcheme(req *http.Request) string {
	if req.TLS != nil {
		return "https"
	}

	return "http"
}

func (*httpRunner) getProtocolVersion(req *http.Request) string {
	major := strconv.Itoa(req.ProtoMajor)
	if req.ProtoMajor == 1 || req.ProtoMinor != 0 {
		return major + "." + strconv.Itoa(req.ProtoMinor)
	}

	return major
}

func (r *httpRunner) handle(w http.ResponseWriter, req *http.Request) {
	methodAttr := semconv.HTTPRequestMethodKey.String(req.Method)
	urlSchemeAttr := semconv.URLScheme(r.getURLcheme(req))
	protoVersionAttr := semconv.NetworkProtocolVersion(r.getProtocolVersion(req))

	ctx := r.env.Tel.ExtractTraceContext(req.Context(), propagation.HeaderCarrier(req.Header))

	ctx, span := r.env.Tel.StartTrace(
		ctx, req.Method,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			methodAttr,
			semconv.URLPath(req.URL.Path),
			urlSchemeAttr,
			protoVersionAttr,
		),
	)
	defer span.End()

	r.env.Metrics.IncrementHttpServerActiveRequests()
	defer r.env.Metrics.DecrementHttpServerActiveRequests()

	rw := &httpResponseWriter{
		ResponseWriter: w,
		timestamp:      time.Now(),
	}

	r.handleRequest(ctx, rw, req)

	// Metrics recording

	statusCodeAttr := semconv.HTTPResponseStatusCode(rw.statusCode)
	span.SetAttributes(statusCodeAttr)

	attributes := r.env.Tel.NewMetricAttributes(
		methodAttr, urlSchemeAttr, statusCodeAttr, protoVersionAttr,
	)

	r.env.Metrics.RecordHttpServerRequestBodySizeWithAttributes(
		ctx, rw.requestBodySize, attributes,
	)

	resBodySize := rw.bytesWritten
	if req.Method == http.MethodHead {
		resBodySize = 0
	}

	r.env.Metrics.RecordHttpServerResponseBodySizeWithAttributes(
		ctx, resBodySize, attributes,
	)

	reqDuration := time.Since(rw.timestamp)
	r.env.Metrics.RecordHttpServerRequestDurationWithAttributes(
		ctx, reqDuration.Seconds(), attributes,
	)
}

func (r *httpRunner) awaitResponse(
	ctx context.Context, fut *future.Future[*link.HTTPFuture],
) (*link.HTTPFuture, future.State, error) {
	r.env.Metrics.IncrementGocciaHttpIngressPendingResponses()
	defer r.env.Metrics.DecrementGocciaHttpIngressPendingResponses()

	// Wait for the response using the future
	futureCtx, cancel := context.WithTimeout(ctx, r.env.responseTimeout)
	defer cancel()

	awaitStartTime := time.Now()

	res, state, err := fut.Await(futureCtx)

	r.env.Metrics.RecordGocciaHttpIngressResponseWaitDuration(
		ctx, time.Since(awaitStartTime).Seconds(),
		future.StateToString(state),
	)

	return res, state, err
}

func (r *httpRunner) handleRequest(ctx context.Context, rw *httpResponseWriter, req *http.Request) {
	defer req.Body.Close()

	body, requestBodySize, ok := r.readBody(rw, req.Body)
	rw.requestBodySize = requestBodySize
	if !ok {
		return
	}

	reqMessage := r.makeRequestMessage(req, body)

	// Set the receive time and the timestamp
	reqMessage.SetReceiveTime(rw.timestamp)
	reqMessage.SetTimestamp(rw.timestamp)

	// Set span context
	reqMessage.SaveSpan(trace.SpanFromContext(ctx))

	// Create the future
	correlationID, fut := r.env.link.NewFuture()
	reqMessage.SetCorrelationID(correlationID)

	// Send the message to the output (next stage)
	if ok := r.writeRequestMessage(ctx, rw, correlationID, reqMessage); !ok {
		return
	}

	// Wait for the response
	resMessage, state, err := r.awaitResponse(ctx, fut)

	switch state {
	case future.StateResolved:
		if resMessage == nil {
			http.Error(rw, "bad gateway", http.StatusBadGateway)
			return
		}

		r.writeResponse(rw, resMessage.GetBody())
		resMessage.Destroy()

	case future.StateRejected:
		http.Error(rw, "bad gateway", http.StatusBadGateway)

	case future.StateTimeout, future.StateCanceled:
		if errors.Is(err, context.DeadlineExceeded) {
			http.Error(rw, "gateway timeout", http.StatusGatewayTimeout)
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

// HTTPStage is an ingress stage that serves HTTP requests and forwards them
// through a request-response pipeline.
type HTTPStage struct {
	*stage.IngressStage[*HTTPMessage, *httpEnv]
}

// NewHTTPStage returns a new HTTP ingress stage using link to correlate requests
// with responses received by the corresponding HTTP egress stage.
func NewHTTPStage(link *link.HTTP, outConnector msgConn[*HTTPMessage], config *HTTPConfig) *HTTPStage {
	return &HTTPStage{
		IngressStage: stage.NewIngressStageFromRunner[*HTTPMessage](
			"http", newHTTPEnv(link, config), newHTTPRunner(outConnector),
		),
	}
}
