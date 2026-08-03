package ingress

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/FerroO2000/goccia/link"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const httpIngressTestTimeout = 5 * time.Second

// ─── Test Doubles ───────────────────────────────────────────────────────────|

type httpIngressWriteResult struct {
	written int
	err     error
}

type httpIngressResponseWriter struct {
	header      http.Header
	statusCodes []int
	results     []httpIngressWriteResult
	writes      [][]byte
}

func newHTTPIngressResponseWriter(results ...httpIngressWriteResult) *httpIngressResponseWriter {
	return &httpIngressResponseWriter{
		header:  make(http.Header),
		results: results,
	}
}

func (w *httpIngressResponseWriter) Header() http.Header {
	return w.header
}

func (w *httpIngressResponseWriter) WriteHeader(statusCode int) {
	w.statusCodes = append(w.statusCodes, statusCode)
}

func (w *httpIngressResponseWriter) Write(body []byte) (int, error) {
	w.writes = append(w.writes, append([]byte(nil), body...))
	result := w.results[len(w.writes)-1]
	return result.written, result.err
}

type httpIngressTrackingBody struct {
	io.Reader
	closed bool
}

func (b *httpIngressTrackingBody) Close() error {
	b.closed = true
	return nil
}

type httpIngressListener struct {
	connections  chan net.Conn
	closed       chan struct{}
	acceptCalled chan struct{}
	acceptOnce   sync.Once
	closeOnce    sync.Once
}

func newHTTPIngressListener() *httpIngressListener {
	return &httpIngressListener{
		connections:  make(chan net.Conn, 1),
		closed:       make(chan struct{}),
		acceptCalled: make(chan struct{}),
	}
}

func (l *httpIngressListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() {
		close(l.acceptCalled)
	})

	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *httpIngressListener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closed)
	})
	return nil
}

func (*httpIngressListener) Addr() net.Addr {
	return httpIngressAddr("in-memory")
}

type httpIngressAddr string

func (a httpIngressAddr) Network() string {
	return string(a)
}

func (a httpIngressAddr) String() string {
	return string(a)
}

func newInitializedHTTPIngressEnv(
	t *testing.T, cfg *HTTPConfig, httpLink *link.HTTP,
) *httpEnv {
	t.Helper()

	testEnv := newHTTPEnv(httpLink, cfg)
	testEnv.SetTelemetry(telemetry.NewTelemetry(stage.KindIngress, "http_test"))
	require.NoError(t, testEnv.Init(t.Context()))
	return testEnv
}

func makeTestTLSCertificate(t *testing.T) tls.Certificate {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certificateDER, err := x509.CreateCertificate(
		rand.Reader, template, template, publicKey, privateKey,
	)
	require.NoError(t, err)

	return tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
	}
}

// ─── Config ─────────────────────────────────────────────────────────────────|

type httpIngressConfigSuite struct {
	suite.Suite
}

func Test_HTTPIngress_Config(t *testing.T) {
	suite.Run(t, new(httpIngressConfigSuite))
}

func (s *httpIngressConfigSuite) Test_Defaults() {
	cfg := NewHTTPConfig()

	s.Equal(&HTTPConfig{
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
	}, cfg)
}

func (s *httpIngressConfigSuite) Test_ValidationRestoresInvalidValues() {
	cfg := &HTTPConfig{
		ReadTimeout:        -time.Second,
		ReadHeaderTimeout:  -time.Second,
		ShutdownTimeout:    -time.Second,
		IdleTimeout:        -time.Second,
		MaxRequestBodySize: -1,
		ResponseTimeout:    -time.Second,
		WriteTimeout:       -time.Second,
		OutputQueueSize:    -1,
	}
	output := connector.NewRingBuffer[*HTTPMessage](1)
	stage := NewHTTPStage(link.NewHTTP(), output, cfg)

	s.Require().NoError(stage.Init(s.T().Context()))
	s.Equal(NewHTTPConfig(), cfg)
}

// ─── Environment ────────────────────────────────────────────────────────────|

type httpIngressEnvironmentSuite struct {
	suite.Suite
}

func Test_HTTPIngress_Environment(t *testing.T) {
	suite.Run(t, new(httpIngressEnvironmentSuite))
}

func (s *httpIngressEnvironmentSuite) Test_InitConfiguresServerAndLimits() {
	cfg := NewHTTPConfig()
	cfg.IPAddr = "127.0.0.1"
	cfg.Port = 43210
	cfg.ReadTimeout = time.Second
	cfg.ReadHeaderTimeout = 2 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	cfg.IdleTimeout = 4 * time.Second
	cfg.ResponseTimeout = 5 * time.Second
	cfg.MaxRequestBodySize = 64

	testEnv := newInitializedHTTPIngressEnv(s.T(), cfg, link.NewHTTP())

	s.Equal("127.0.0.1:43210", testEnv.server.Addr)
	s.Equal(time.Second, testEnv.server.ReadTimeout)
	s.Equal(2*time.Second, testEnv.server.ReadHeaderTimeout)
	s.Equal(3*time.Second, testEnv.server.WriteTimeout)
	s.Equal(4*time.Second, testEnv.server.IdleTimeout)
	s.Equal(5*time.Second, testEnv.responseTimeout)
	s.Equal(int64(64), testEnv.maxRequestBodySize)
	s.False(testEnv.tlsEnabled)
	s.Nil(testEnv.server.TLSConfig)
}

func (s *httpIngressEnvironmentSuite) Test_InitRejectsInvalidTLSConfiguration() {
	tests := []struct {
		name      string
		tlsConfig *tls.Config
		wantError string
	}{
		{
			name:      "missing TLS config",
			wantError: "HTTP TLS is enabled but its configuration is missing",
		},
		{
			name:      "missing certificate source",
			tlsConfig: &tls.Config{},
			wantError: "HTTP TLS requires a certificate or GetCertificate callback",
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			cfg := NewHTTPConfig()
			cfg.TLSEnabled = true
			cfg.TLSConfig = test.tlsConfig
			testEnv := newHTTPEnv(link.NewHTTP(), cfg)
			testEnv.SetTelemetry(telemetry.NewTelemetry(stage.KindIngress, "http_test"))

			s.EqualError(testEnv.Init(s.T().Context()), test.wantError)
			s.NotNil(testEnv.server)
			s.False(testEnv.tlsEnabled)
		})
	}
}

func (s *httpIngressEnvironmentSuite) Test_TLSConfigIsClonedAndDefaultsToTLS12() {
	originalTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{makeTestTLSCertificate(s.T())},
	}
	cfg := NewHTTPConfig()
	cfg.TLSEnabled = true
	cfg.TLSConfig = originalTLSConfig

	testEnv := newInitializedHTTPIngressEnv(s.T(), cfg, link.NewHTTP())

	s.True(testEnv.tlsEnabled)
	s.NotSame(originalTLSConfig, testEnv.server.TLSConfig)
	s.Equal(uint16(tls.VersionTLS12), testEnv.server.TLSConfig.MinVersion)
	s.Len(testEnv.server.TLSConfig.Certificates, 1)
	s.Zero(originalTLSConfig.MinVersion)
}

func (s *httpIngressEnvironmentSuite) Test_TLSConfigAcceptsDynamicCertificate() {
	getCertificate := func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		return nil, nil
	}
	cfg := NewHTTPConfig()
	cfg.TLSEnabled = true
	cfg.TLSConfig = &tls.Config{
		MinVersion:     tls.VersionTLS13,
		GetCertificate: getCertificate,
	}

	testEnv := newInitializedHTTPIngressEnv(s.T(), cfg, link.NewHTTP())

	s.True(testEnv.tlsEnabled)
	s.Equal(uint16(tls.VersionTLS13), testEnv.server.TLSConfig.MinVersion)
	s.NotNil(testEnv.server.TLSConfig.GetCertificate)
}

// ─── Response Writer ────────────────────────────────────────────────────────|

type httpIngressResponseWriterSuite struct {
	suite.Suite
}

func Test_HTTPIngress_ResponseWriter(t *testing.T) {
	suite.Run(t, new(httpIngressResponseWriterSuite))
}

func (s *httpIngressResponseWriterSuite) Test_WriteHeaderOnlyForwardsTheFirstStatus() {
	underlying := newHTTPIngressResponseWriter()
	w := &httpResponseWriter{ResponseWriter: underlying}

	w.WriteHeader(http.StatusCreated)
	w.WriteHeader(http.StatusInternalServerError)

	s.Equal(http.StatusCreated, w.statusCode)
	s.Equal([]int{http.StatusCreated}, underlying.statusCodes)
}

func (s *httpIngressResponseWriterSuite) Test_WriteTracksSuccessfulBytesAndImplicitStatus() {
	underlying := newHTTPIngressResponseWriter(
		httpIngressWriteResult{written: 3},
		httpIngressWriteResult{written: 2},
	)
	w := &httpResponseWriter{ResponseWriter: underlying}

	written, err := w.Write([]byte("one"))
	s.Require().NoError(err)
	s.Equal(3, written)
	written, err = w.Write([]byte("tw"))
	s.Require().NoError(err)
	s.Equal(2, written)

	s.Equal(http.StatusOK, w.statusCode)
	s.Equal(int64(5), w.bytesWritten)
	s.NoError(w.writeErr)
}

func (s *httpIngressResponseWriterSuite) Test_WriteRetainsTheFirstError() {
	firstErr := errors.New("first write failed")
	secondErr := errors.New("second write failed")
	underlying := newHTTPIngressResponseWriter(
		httpIngressWriteResult{written: 2, err: firstErr},
		httpIngressWriteResult{written: 1, err: secondErr},
	)
	w := &httpResponseWriter{ResponseWriter: underlying}

	written, err := w.Write([]byte("first"))
	s.Equal(2, written)
	s.ErrorIs(err, firstErr)
	written, err = w.Write([]byte("second"))
	s.Equal(1, written)
	s.ErrorIs(err, secondErr)

	s.Equal(int64(3), w.bytesWritten)
	s.ErrorIs(w.writeErr, firstErr)
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

type httpIngressRunnerSuite struct {
	suite.Suite
}

func Test_HTTPIngress_Runner(t *testing.T) {
	suite.Run(t, new(httpIngressRunnerSuite))
}

func (s *httpIngressRunnerSuite) Test_InitCreatesFanInQueue() {
	cfg := NewHTTPConfig()
	cfg.OutputQueueSize = 7
	testEnv := newInitializedHTTPIngressEnv(s.T(), cfg, link.NewHTTP())
	runner := newHTTPRunner(connector.NewRingBuffer[*HTTPMessage](1))
	runner.SetEnvironment(testEnv)

	s.Require().NoError(runner.Init(s.T().Context()))
	s.NotNil(runner.fanIn)
	s.Equal(uint64(0), runner.fanIn.Len())
	s.NotNil(runner.runServerDone)
}

func (s *httpIngressRunnerSuite) Test_ReadBodyCountsObservedBytes() {
	tests := []struct {
		name          string
		body          string
		maxBodySize   int64
		wantBody      string
		wantBytesRead int64
		wantOK        bool
		wantStatus    int
	}{
		{
			name:          "empty body",
			maxBodySize:   4,
			wantBytesRead: 0,
			wantOK:        true,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "body within limit",
			body:          "data",
			maxBodySize:   4,
			wantBody:      "data",
			wantBytesRead: 4,
			wantOK:        true,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "body over limit",
			body:          "excess",
			maxBodySize:   4,
			wantBytesRead: 4,
			wantOK:        false,
			wantStatus:    http.StatusRequestEntityTooLarge,
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			runner := newHTTPRunner(nil)
			runner.SetEnvironment(&httpEnv{maxRequestBodySize: test.maxBodySize})
			resWriter := httptest.NewRecorder()

			body, bytesRead, ok := runner.readBody(
				resWriter, io.NopCloser(strings.NewReader(test.body)),
			)

			s.Equal(test.wantBody, string(body))
			s.Equal(test.wantBytesRead, bytesRead)
			s.Equal(test.wantOK, ok)
			s.Equal(test.wantStatus, resWriter.Code)
		})
	}
}

func (s *httpIngressRunnerSuite) Test_MakeRequestMessageCopiesRequestData() {
	req := httptest.NewRequest(http.MethodPatch, "http://example.test/items?q=go", strings.NewReader("body"))
	req.RemoteAddr = "192.0.2.1:1234"
	req.Header.Add("X-Test", "first")
	runner := newHTTPRunner(nil)

	reqMessage := runner.makeRequestMessage(req, []byte("body"))
	req.Header.Add("X-Test", "second")

	s.Equal(&HTTPMessage{
		Method:     http.MethodPatch,
		Path:       "/items",
		Query:      "q=go",
		Header:     http.Header{"X-Test": []string{"first"}},
		Body:       []byte("body"),
		RemoteAddr: "192.0.2.1:1234",
	}, reqMessage.GetBody())
	reqMessage.Destroy()
}

func (s *httpIngressRunnerSuite) Test_WriteRequestMessageEnqueuesAndRejects() {
	tests := []struct {
		name       string
		closeQueue bool
		wantOK     bool
		wantStatus int
	}{
		{name: "enqueued", wantOK: true, wantStatus: http.StatusOK},
		{name: "closed queue", closeQueue: true, wantOK: false, wantStatus: http.StatusServiceUnavailable},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			httpLink := link.NewHTTP()
			testEnv := newInitializedHTTPIngressEnv(s.T(), NewHTTPConfig(), httpLink)
			runner := newHTTPRunner(nil)
			runner.SetEnvironment(testEnv)
			s.Require().NoError(runner.Init(s.T().Context()))
			s.T().Cleanup(runner.fanIn.Close)

			correlationID, _ := httpLink.NewFuture()
			requestBody := &HTTPMessage{Method: http.MethodPost, Body: []byte("body")}
			reqMessage := message.NewMessage(requestBody)
			if test.closeQueue {
				runner.fanIn.Close()
			}
			resWriter := httptest.NewRecorder()

			ok := runner.writeRequestMessage(
				s.T().Context(), resWriter, correlationID, reqMessage,
			)

			s.Equal(test.wantOK, ok)
			s.Equal(test.wantStatus, resWriter.Code)
			if test.wantOK {
				queued, err := runner.fanIn.Read(s.T().Context())
				s.Require().NoError(err)
				s.Same(reqMessage, queued)
				s.True(httpLink.RejectFuture(correlationID, errors.New("test cleanup")))
				queued.Destroy()
			} else {
				s.Empty(requestBody.Method)
				s.Nil(requestBody.Body)
				s.False(httpLink.RejectFuture(correlationID, errors.New("already rejected")))
			}
		})
	}
}

func (s *httpIngressRunnerSuite) Test_WriteResponseCopiesHeadersStatusAndBody() {
	runner := newHTTPRunner(nil)
	recorder := httptest.NewRecorder()
	response := &message.HTTPResponse{
		StatusCode: http.StatusAccepted,
		Header: http.Header{
			"X-Value": []string{"first", "second"},
		},
		Body: []byte("accepted"),
	}

	runner.writeResponse(recorder, response)

	s.Equal(http.StatusAccepted, recorder.Code)
	s.Equal([]string{"first", "second"}, recorder.Header().Values("X-Value"))
	s.Equal("accepted", recorder.Body.String())
}

func (s *httpIngressRunnerSuite) Test_RequestURLScheme() {
	runner := newHTTPRunner(nil)

	s.Equal("http", runner.getURLcheme(&http.Request{}))
	s.Equal("https", runner.getURLcheme(&http.Request{TLS: &tls.ConnectionState{}}))
}

func (s *httpIngressRunnerSuite) Test_RequestProtocolVersion() {
	runner := newHTTPRunner(nil)
	tests := []struct {
		major int
		minor int
		want  string
	}{
		{major: 1, minor: 0, want: "1.0"},
		{major: 1, minor: 1, want: "1.1"},
		{major: 2, minor: 0, want: "2"},
		{major: 2, minor: 1, want: "2.1"},
		{major: 3, minor: 0, want: "3"},
	}

	for _, test := range tests {
		s.Equal(test.want, runner.getProtocolVersion(&http.Request{
			ProtoMajor: test.major,
			ProtoMinor: test.minor,
		}))
	}
}

func (s *httpIngressRunnerSuite) Test_ListenAndServeReturnsListenerErrorsForHTTPAndHTTPS() {
	tests := []struct {
		name       string
		tlsEnabled bool
		tlsConfig  *tls.Config
	}{
		{name: "HTTP"},
		{
			name:       "HTTPS",
			tlsEnabled: true,
			tlsConfig: &tls.Config{
				Certificates: []tls.Certificate{makeTestTLSCertificate(s.T())},
			},
		},
	}

	for _, test := range tests {
		s.Run(test.name, func() {
			runner := newHTTPRunner(nil)
			runner.SetEnvironment(&httpEnv{
				server: &http.Server{
					Addr:      "invalid address",
					TLSConfig: test.tlsConfig,
				},
				tlsEnabled: test.tlsEnabled,
			})

			s.Error(runner.listenAndServe())
		})
	}
}

func (s *httpIngressRunnerSuite) Test_RunServerSignalsUnexpectedFailure() {
	testEnv := newHTTPEnv(link.NewHTTP(), NewHTTPConfig())
	testEnv.SetTelemetry(telemetry.NewTelemetry(stage.KindIngress, "http_test"))
	testEnv.server = &http.Server{Addr: "invalid address"}
	runner := newHTTPRunner(nil)
	runner.SetEnvironment(testEnv)

	runner.runServer()

	_, open := <-runner.runServerDone
	s.True(open)
	_, open = <-runner.runServerDone
	s.False(open)
	s.NotNil(testEnv.server.Handler)
}

func (s *httpIngressRunnerSuite) Test_RunServerIgnoresExpectedClosure() {
	server := &http.Server{Addr: "127.0.0.1:0"}
	s.Require().NoError(server.Close())
	testEnv := newHTTPEnv(link.NewHTTP(), NewHTTPConfig())
	testEnv.SetTelemetry(telemetry.NewTelemetry(stage.KindIngress, "http_test"))
	testEnv.server = server
	runner := newHTTPRunner(nil)
	runner.SetEnvironment(testEnv)

	runner.runServer()

	_, open := <-runner.runServerDone
	s.False(open)
}

func (s *httpIngressRunnerSuite) Test_ShutdownServerStopsServing() {
	listener := newHTTPIngressListener()
	cfg := NewHTTPConfig()
	cfg.ShutdownTimeout = time.Second
	testEnv := newHTTPEnv(link.NewHTTP(), cfg)
	testEnv.SetTelemetry(telemetry.NewTelemetry(stage.KindIngress, "http_test"))
	testEnv.server = &http.Server{}
	runner := newHTTPRunner(nil)
	runner.SetEnvironment(testEnv)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- testEnv.server.Serve(listener)
		close(runner.runServerDone)
	}()
	s.requireListenerWaiting(listener)

	runner.shutdownServer()

	s.ErrorIs(<-serveErr, http.ErrServerClosed)
}

func (s *httpIngressRunnerSuite) Test_ShutdownServerForceClosesActiveRequestsAfterTimeout() {
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	listener := newHTTPIngressListener()
	cfg := NewHTTPConfig()
	cfg.ShutdownTimeout = time.Nanosecond
	testEnv := newHTTPEnv(link.NewHTTP(), cfg)
	testEnv.SetTelemetry(telemetry.NewTelemetry(stage.KindIngress, "http_test"))
	testEnv.server = &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		close(requestStarted)
		<-req.Context().Done()
		close(requestCanceled)
	})}
	runner := newHTTPRunner(nil)
	runner.SetEnvironment(testEnv)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- testEnv.server.Serve(listener)
		close(runner.runServerDone)
	}()
	s.requireListenerWaiting(listener)
	serverConnection, clientConnection := net.Pipe()
	listener.connections <- serverConnection
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		defer clientConnection.Close()
		_, _ = io.WriteString(clientConnection, "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n")
		_, _ = io.Copy(io.Discard, clientConnection)
	}()

	s.Require().Eventually(func() bool {
		select {
		case <-requestStarted:
			return true
		default:
			return false
		}
	}, httpIngressTestTimeout, time.Millisecond)
	runner.shutdownServer()

	s.ErrorIs(<-serveErr, http.ErrServerClosed)
	s.Require().Eventually(func() bool {
		select {
		case <-requestCanceled:
			return true
		default:
			return false
		}
	}, httpIngressTestTimeout, time.Millisecond)
	s.Require().Eventually(func() bool {
		select {
		case <-clientDone:
			return true
		default:
			return false
		}
	}, httpIngressTestTimeout, time.Millisecond)
}

func (s *httpIngressRunnerSuite) requireListenerWaiting(listener *httpIngressListener) {
	s.T().Helper()

	s.Require().Eventually(func() bool {
		select {
		case <-listener.acceptCalled:
			return true
		default:
			return false
		}
	}, httpIngressTestTimeout, time.Millisecond)
}

func (s *httpIngressRunnerSuite) Test_RunStopsOnCancellationAndDrains() {
	output := connector.NewRingBuffer[*HTTPMessage](1)
	cfg := NewHTTPConfig()
	cfg.IPAddr = "127.0.0.1"
	cfg.Port = 0
	testEnv := newInitializedHTTPIngressEnv(s.T(), cfg, link.NewHTTP())
	// Validation intentionally replaces port zero, so use an ephemeral port for
	// this isolated runner after the environment has initialized.
	testEnv.server.Addr = "127.0.0.1:0"
	runner := newHTTPRunner(output)
	runner.SetEnvironment(testEnv)
	s.Require().NoError(runner.Init(s.T().Context()))
	runCtx, cancel := context.WithCancel(s.T().Context())
	cancel()
	runDone := make(chan struct{})
	go func() {
		runner.Run(runCtx)
		close(runDone)
	}()

	s.Require().Eventually(func() bool {
		select {
		case <-runDone:
			return true
		default:
			return false
		}
	}, httpIngressTestTimeout, time.Millisecond)
	runner.Close(s.T().Context())
}

// ─── Request Handling ───────────────────────────────────────────────────────|

type httpIngressRequestSuite struct {
	suite.Suite

	runner   *httpRunner
	testEnv  *httpEnv
	httpLink *link.HTTP
}

func Test_HTTPIngress_RequestHandling(t *testing.T) {
	suite.Run(t, new(httpIngressRequestSuite))
}

func (s *httpIngressRequestSuite) SetupTest() {
	cfg := NewHTTPConfig()
	cfg.MaxRequestBodySize = 8
	cfg.ResponseTimeout = time.Second
	s.httpLink = link.NewHTTP()
	s.testEnv = newInitializedHTTPIngressEnv(s.T(), cfg, s.httpLink)
	s.runner = newHTTPRunner(nil)
	s.runner.SetEnvironment(s.testEnv)
	s.Require().NoError(s.runner.Init(s.T().Context()))
	s.T().Cleanup(s.runner.fanIn.Close)
}

func (s *httpIngressRequestSuite) readQueuedRequest() *msg[*HTTPMessage] {
	s.T().Helper()

	ctx, cancel := context.WithTimeout(s.T().Context(), httpIngressTestTimeout)
	defer cancel()
	reqMessage, err := s.runner.fanIn.Read(ctx)
	s.Require().NoError(err)
	return reqMessage
}

func (s *httpIngressRequestSuite) requireDone(done <-chan struct{}) {
	s.T().Helper()

	s.Require().Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, httpIngressTestTimeout, time.Millisecond)
}

func (s *httpIngressRequestSuite) startHandleRequest(
	ctx context.Context, req *http.Request,
) (*httptest.ResponseRecorder, *httpResponseWriter, <-chan struct{}) {
	s.T().Helper()

	recorder := httptest.NewRecorder()
	rw := &httpResponseWriter{
		ResponseWriter: recorder,
		timestamp:      time.Now(),
	}
	done := make(chan struct{})
	go func() {
		s.runner.handleRequest(ctx, rw, req)
		close(done)
	}()
	return recorder, rw, done
}

func (s *httpIngressRequestSuite) Test_HandleRecordsAndReturnsResolvedResponse() {
	body := &httpIngressTrackingBody{Reader: strings.NewReader("request")}
	req := httptest.NewRequest(http.MethodHead, "https://example.test/items?limit=2", body)
	req.TLS = &tls.ConnectionState{}
	req.RemoteAddr = "192.0.2.2:4321"
	req.Header.Add("X-Request", "value")
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		s.runner.handle(recorder, req)
		close(done)
	}()
	reqMessage := s.readQueuedRequest()
	responseBody := &message.HTTPResponse{
		StatusCode: http.StatusCreated,
		Header:     http.Header{"X-Response": []string{"one", "two"}},
		Body:       []byte("response"),
	}

	s.True(s.httpLink.ResolveFuture(
		reqMessage.GetCorrelationID(), message.NewMessage(responseBody),
	))
	s.requireDone(done)

	s.Equal(http.StatusCreated, recorder.Code)
	s.Equal("response", recorder.Body.String())
	s.Equal([]string{"one", "two"}, recorder.Header().Values("X-Response"))
	s.True(body.closed)
	s.Equal(http.MethodHead, reqMessage.GetBody().Method)
	s.Equal("/items", reqMessage.GetBody().Path)
	s.Equal("limit=2", reqMessage.GetBody().Query)
	s.Equal("192.0.2.2:4321", reqMessage.GetBody().RemoteAddr)
	s.Equal([]byte("request"), reqMessage.GetBody().Body)
	s.Equal("value", reqMessage.GetBody().Header.Get("X-Request"))
	s.False(reqMessage.GetReceiveTime().IsZero())
	s.Equal(reqMessage.GetReceiveTime(), reqMessage.GetTimestamp())
	s.Zero(responseBody.StatusCode)
	s.Nil(responseBody.Header)
	s.Nil(responseBody.Body)
	reqMessage.Destroy()
}

func (s *httpIngressRequestSuite) Test_HandleRequestRejectsOversizedBody() {
	body := &httpIngressTrackingBody{Reader: strings.NewReader("too large")}
	req := httptest.NewRequest(http.MethodPost, "http://example.test", body)
	recorder := httptest.NewRecorder()
	rw := &httpResponseWriter{ResponseWriter: recorder, timestamp: time.Now()}

	s.runner.handleRequest(s.T().Context(), rw, req)

	s.Equal(http.StatusRequestEntityTooLarge, recorder.Code)
	s.Equal(int64(8), rw.requestBodySize)
	s.True(body.closed)
	s.Equal(uint64(0), s.runner.fanIn.Len())
}

func (s *httpIngressRequestSuite) Test_HandleRequestRejectsClosedQueue() {
	s.runner.fanIn.Close()
	body := &httpIngressTrackingBody{Reader: strings.NewReader("request")}
	req := httptest.NewRequest(http.MethodPost, "http://example.test", body)
	recorder := httptest.NewRecorder()
	rw := &httpResponseWriter{ResponseWriter: recorder, timestamp: time.Now()}

	s.runner.handleRequest(s.T().Context(), rw, req)

	s.Equal(http.StatusServiceUnavailable, recorder.Code)
	s.Equal("service unavailable\n", recorder.Body.String())
	s.True(body.closed)
}

func (s *httpIngressRequestSuite) Test_HandleRequestReturnsBadGatewayForNilResponse() {
	req := httptest.NewRequest(http.MethodGet, "http://example.test", nil)
	recorder, _, done := s.startHandleRequest(s.T().Context(), req)
	reqMessage := s.readQueuedRequest()

	s.True(s.httpLink.ResolveFuture(reqMessage.GetCorrelationID(), nil))
	s.requireDone(done)

	s.Equal(http.StatusBadGateway, recorder.Code)
	s.Equal("bad gateway\n", recorder.Body.String())
	reqMessage.Destroy()
}

func (s *httpIngressRequestSuite) Test_HandleRequestReturnsBadGatewayForRejectedResponse() {
	req := httptest.NewRequest(http.MethodGet, "http://example.test", nil)
	recorder, _, done := s.startHandleRequest(s.T().Context(), req)
	reqMessage := s.readQueuedRequest()

	s.True(s.httpLink.RejectFuture(
		reqMessage.GetCorrelationID(), errors.New("downstream failure"),
	))
	s.requireDone(done)

	s.Equal(http.StatusBadGateway, recorder.Code)
	s.Equal("bad gateway\n", recorder.Body.String())
	reqMessage.Destroy()
}

func (s *httpIngressRequestSuite) Test_HandleRequestReturnsGatewayTimeoutAndDeletesFuture() {
	s.testEnv.responseTimeout = time.Nanosecond
	req := httptest.NewRequest(http.MethodGet, "http://example.test", nil)
	recorder := httptest.NewRecorder()
	rw := &httpResponseWriter{ResponseWriter: recorder, timestamp: time.Now()}

	s.runner.handleRequest(s.T().Context(), rw, req)
	reqMessage := s.readQueuedRequest()

	s.Equal(http.StatusGatewayTimeout, recorder.Code)
	s.Equal("gateway timeout\n", recorder.Body.String())
	s.False(s.httpLink.ResolveFuture(reqMessage.GetCorrelationID(), nil))
	reqMessage.Destroy()
}

func (s *httpIngressRequestSuite) Test_HandleRequestCancellationDeletesFutureWithoutWritingResponse() {
	ctx, cancel := context.WithCancel(s.T().Context())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "http://example.test", nil)
	recorder := httptest.NewRecorder()
	rw := &httpResponseWriter{ResponseWriter: recorder, timestamp: time.Now()}

	s.runner.handleRequest(ctx, rw, req)
	reqMessage := s.readQueuedRequest()

	s.Zero(rw.statusCode)
	s.Empty(recorder.Body.String())
	s.False(s.httpLink.ResolveFuture(reqMessage.GetCorrelationID(), nil))
	reqMessage.Destroy()
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

type httpIngressStageSuite struct {
	suite.Suite
}

func Test_HTTPIngress_Stage(t *testing.T) {
	suite.Run(t, new(httpIngressStageSuite))
}

func (s *httpIngressStageSuite) Test_ConstructsAndInitializesStage() {
	cfg := NewHTTPConfig()
	cfg.IPAddr = "127.0.0.1"
	cfg.Port = 9000
	output := connector.NewRingBuffer[*HTTPMessage](2)
	httpStage := NewHTTPStage(link.NewHTTP(), output, cfg)

	s.Equal(stage.KindIngress, httpStage.Kind())
	s.Equal("http", httpStage.Name())
	s.Same(cfg, httpStage.Env().Config)
	s.Empty(httpStage.Inputs())
	s.Len(httpStage.Outputs(), 1)
	s.Require().NoError(httpStage.Init(s.T().Context()))
	s.Equal("127.0.0.1:9000", httpStage.Env().server.Addr)
}
