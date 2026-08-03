package egress

import (
	"context"
	"errors"
	"net/http"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/link"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// HTTPConfig contains the configuration for the HTTP egress stage.
type HTTPConfig struct {
	// Header contains headers added to every response before it is delivered to
	// the corresponding HTTP ingress stage. A nil header adds nothing.
	Header http.Header
}

// NewHTTPConfig returns the default configuration for the HTTP egress stage.
func NewHTTPConfig() *HTTPConfig {
	return &HTTPConfig{
		Header: nil,
	}
}

// Validate checks the configuration.
func (*HTTPConfig) Validate(_ *config.AnomalyCollector) {}

// ─── Message ────────────────────────────────────────────────────────────────|

// HTTPMessage is the response message consumed by the HTTP egress stage.
type HTTPMessage = message.HTTPResponse

// ─── Environment ────────────────────────────────────────────────────────────|

type httpEnv struct {
	*env.BaseEnv[*HTTPConfig, *metrics.EmptyMetrics]

	link *link.HTTP
}

func newHTTPEnv(link *link.HTTP, config *HTTPConfig) *httpEnv {
	return &httpEnv{
		BaseEnv: env.NewEgressEnv(config, metrics.NewEmptyMetrics()),

		link: link,
	}
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*httpEnv] = (*httpRunner)(nil)

type httpRunner struct {
	*httpEnv

	inConnector msgConn[*HTTPMessage]

	runDone chan struct{}
}

func newHTTPRunner(inConnector msgConn[*HTTPMessage]) *httpRunner {
	return &httpRunner{
		inConnector: inConnector,

		runDone: make(chan struct{}),
	}
}

func (r *httpRunner) SetEnvironment(env *httpEnv) {
	r.httpEnv = env
}

func (r *httpRunner) Init(_ context.Context) error {
	return nil
}

func (r *httpRunner) Run(ctx context.Context) {
	defer close(r.runDone)

	for {
		msgIn, err := r.inConnector.Read(ctx)
		if err != nil {
			return
		}

		r.handleResponse(ctx, msgIn)
	}
}

func (r *httpRunner) checkStatusCode(statusCode int) error {
	if statusCode < 200 || statusCode >= 600 {
		return errors.New("invalid http status code")
	}
	return nil
}

func (r *httpRunner) resolveResponse(correlationID uint64, msgIn *msg[*HTTPMessage]) {
	if !r.link.ResolveFuture(correlationID, msgIn) {
		r.Tel.LogWarn("failed to resolve request, future not found", "correlation_id", correlationID)
		msgIn.Destroy()
	}
}

func (r *httpRunner) rejectResponse(correlationID uint64, msgIn *msg[*HTTPMessage], err error) {
	if !r.link.RejectFuture(correlationID, err) {
		r.Tel.LogWarn("failed to reject request, future not found", "correlation_id", correlationID)
	}

	// Always destroy the input message
	msgIn.Destroy()
}

func (r *httpRunner) handleHeader(httpResponse *HTTPMessage) {
	baseHeader := r.Config.Header
	if baseHeader == nil {
		return
	}

	for key, values := range baseHeader {
		for _, value := range values {
			httpResponse.Header.Add(key, value)
		}
	}
}

func (r *httpRunner) handleResponse(ctx context.Context, msgIn *msg[*HTTPMessage]) {
	ctx = msgIn.LoadSpanContext(ctx)
	ctx, span := r.Tel.StartTrace(ctx, "handle response")
	defer span.End()

	correlationID := msgIn.GetCorrelationID()
	httpResponse := msgIn.GetBody()

	if err := r.checkStatusCode(httpResponse.StatusCode); err != nil {
		r.Tel.LogError("invalid status code", err, "correlation_id", correlationID)
		r.rejectResponse(correlationID, msgIn, err)
		return
	}

	r.handleHeader(httpResponse)
	r.resolveResponse(correlationID, msgIn)
}

func (r *httpRunner) Close(_ context.Context) {
	<-r.runDone
}

func (r *httpRunner) Inputs() []uintptr {
	return []uintptr{connector.GetConnectorID(r.inConnector)}
}

func (r *httpRunner) Outputs() []uintptr {
	return []uintptr{}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// HTTPStage is an egress stage that delivers correlated responses to the
// corresponding HTTP ingress stage.
type HTTPStage struct {
	*stage.EgressStage[*HTTPMessage, *httpEnv]
}

// NewHTTPStage returns a new HTTP egress stage using link to resolve responses
// for requests accepted by the corresponding HTTP ingress stage.
func NewHTTPStage(link *link.HTTP, outConnector msgConn[*HTTPMessage], config *HTTPConfig) *HTTPStage {
	return &HTTPStage{
		EgressStage: stage.NewEgressStageFromRunner[*HTTPMessage](
			"http", newHTTPEnv(link, config), newHTTPRunner(outConnector),
		),
	}
}
