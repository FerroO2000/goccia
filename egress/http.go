package egress

import (
	"context"
	"errors"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/link"
)

// ─── Message ────────────────────────────────────────────────────────────────|

type HTTPMessage = message.HTTPResponse

// ─── Environment ────────────────────────────────────────────────────────────|

type httpEnv struct {
	*env.BaseEnv[*config.Empty, *metrics.EmptyMetrics]

	link *link.HTTP
}

func newHTTPEnv(link *link.HTTP) *httpEnv {
	return &httpEnv{
		BaseEnv: env.NewEgressEnv(config.NewEmpty(), metrics.NewEmptyMetrics()),

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

		r.handleResponse(msgIn)
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

func (r *httpRunner) handleResponse(msgIn *msg[*HTTPMessage]) {
	correlationID := msgIn.GetCorrelationID()
	httpResponse := msgIn.GetBody()

	if err := r.checkStatusCode(httpResponse.StatusCode); err != nil {
		r.Tel.LogError("invalid status code", err, "correlation_id", correlationID)
		r.rejectResponse(correlationID, msgIn, err)
		return
	}

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

type HTTPStage struct {
	*stage.EgressStage[*HTTPMessage, *httpEnv]
}

func NewHTTPStage(link *link.HTTP, outConnector msgConn[*HTTPMessage]) *HTTPStage {
	return &HTTPStage{
		EgressStage: stage.NewEgressStageFromRunner[*HTTPMessage](
			"http", newHTTPEnv(link), newHTTPRunner(outConnector),
		),
	}
}
