package egress

import (
	"context"
	"errors"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/link"
)

// ─── Message ────────────────────────────────────────────────────────────────|

type HTTPMessage = link.HTTPResponseMessage

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

		r.handle(msgIn)
	}
}

func (r *httpRunner) handle(msgIn *msg[*link.HTTPResponseMessage]) {
	correlationID := msgIn.GetCorrelationID()
	res := msgIn.GetBody()

	// Check the status code
	if res.StatusCode < 200 || res.StatusCode >= 600 {
		err := errors.New("invalid http status code")

		r.Tel.LogError("invalid status code", err, "correlation_id", correlationID)

		if !r.link.RejectFuture(correlationID, err) {
			r.Tel.LogWarn("request has no future", "correlation_id", correlationID)
		}

		msgIn.Destroy()
		return
	}

	if !r.link.ResolveFuture(correlationID, msgIn) {
		r.Tel.LogWarn("request has no future", "correlation_id", correlationID)
		msgIn.Destroy()
	}
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
	*stage.EgressStage[*link.HTTPResponseMessage, *httpEnv]
}

func NewHTTPStage(link *link.HTTP, outConnector msgConn[*HTTPMessage]) *HTTPStage {
	return &HTTPStage{
		EgressStage: stage.NewEgressStageFromRunner[*HTTPMessage](
			"http", newHTTPEnv(link), newHTTPRunner(outConnector),
		),
	}
}
