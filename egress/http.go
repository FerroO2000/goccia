package egress

import (
	"context"

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
		if err != nil && ctx.Err() != nil {
			return
		}

		r.handle(msgIn)
	}
}

func (r *httpRunner) handle(msgIn *msg[*link.HTTPResponseMessage]) {
	correlationID := msgIn.GetCorrelationID()

	if !r.link.ResolveFuture(correlationID, msgIn.GetBody()) {
		r.Tel.LogWarn("request has no future", "correlation_id", correlationID)
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
	*stage.IngressStage[*link.HTTPResponseMessage, *httpEnv]
}

func NewHTTPStage(link *link.HTTP, outConnector msgConn[*HTTPMessage]) *HTTPStage {
	return &HTTPStage{
		IngressStage: stage.NewIngressStageFromRunner[*HTTPMessage](
			"http", newHTTPEnv(link), newHTTPRunner(outConnector),
		),
	}
}
