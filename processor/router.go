package processor

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/telemetry"
	"github.com/FerroO2000/goccia/processor/metrics"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Types ──────────────────────────────────────────────────────────────────|

// RouterFn is a function that takes a message body and returns the ID of the route
// to which the message should be sent. The ID must be a non-negative integer less than the number of routes.
type RouterFn[T msgBody] func(T) int

// ─── Environment ────────────────────────────────────────────────────────────|

type routerEnv[T msgBody] struct {
	*env.BaseEnv[*config.Empty, *metrics.RouterStage]

	routeFn RouterFn[T]

	routes        int
	outConnectors []msgConn[T]
	routeNames    []string

	// Additional metrics
	messagesPerRoute []*atomic.Int64
}

func newRouterEnv[T msgBody](routeFn func(T) int) *routerEnv[T] {
	return &routerEnv[T]{
		BaseEnv: env.NewProcessorEnv(config.NewEmpty(), metrics.NewRouterStage()),

		routeFn: routeFn,

		outConnectors: []msgConn[T]{},
		routeNames:    []string{},

		messagesPerRoute: []*atomic.Int64{},
	}
}

func (e *routerEnv[T]) addOutConnector(name string, outConnector msgConn[T]) int {
	id := e.routes

	e.outConnectors = append(e.outConnectors, outConnector)
	e.routeNames = append(e.routeNames, name)
	e.messagesPerRoute = append(e.messagesPerRoute, &atomic.Int64{})

	e.routes++

	return id
}

func (e *routerEnv[T]) getRoute(id int) (string, msgConn[T], *atomic.Int64) {
	return e.routeNames[id], e.outConnectors[id], e.messagesPerRoute[id]
}

func (e *routerEnv[T]) initAdditionalMetrics() error {
	dataPoints := make([]telemetry.CounterMetricDataPoint, 0, len(e.messagesPerRoute))
	for id := range e.messagesPerRoute {
		routeID := id
		dataPoints = append(dataPoints, telemetry.CounterMetricDataPoint{
			Getter: func() int64 {
				return e.messagesPerRoute[routeID].Load()
			},
			Attributes: []attribute.KeyValue{
				attribute.Int("route_id", routeID),
				attribute.String("route_name", e.routeNames[routeID]),
			},
		})
	}

	return e.Tel.NewCounterMetricSet("routed_messages_per_route", dataPoints)
}

func (e *routerEnv[T]) Init(ctx context.Context) error {
	if err := e.BaseEnv.Init(ctx); err != nil {
		return err
	}

	if e.routeFn == nil {
		return errors.New("route function is not defined")
	}

	if e.routes == 0 {
		return errors.New("no output connector specified")
	}

	return e.initAdditionalMetrics()
}

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*routerEnv[msgBody]] = (*routerRunner[msgBody])(nil)

type routerRunner[T msgBody] struct {
	*routerEnv[T]

	inConnector msgConn[T]

	runDone chan struct{}
}

func newRouterRunner[T msgBody](inConnector msgConn[T]) *routerRunner[T] {
	return &routerRunner[T]{
		inConnector: inConnector,

		runDone: make(chan struct{}),
	}
}

func (r *routerRunner[T]) SetEnvironment(env *routerEnv[T]) {
	r.routerEnv = env
}

func (r *routerRunner[T]) Init(_ context.Context) error {
	return nil
}

func (r *routerRunner[T]) checkRouteID(id int) error {
	if id < 0 || id >= r.routes {
		return errors.New("invalid route ID")
	}
	return nil
}

func (r *routerRunner[T]) Run(ctx context.Context) {
	defer close(r.runDone)

	for {
		msgIn, err := r.inConnector.Read(ctx)
		if err != nil {
			// This means the input connector is closed
			// and there are no more messages in it
			return
		}

		if err := r.route(ctx, msgIn); err != nil {
			r.GetProcessorMetrics().IncrementProcessingErrors()
			r.GetProcessorMetrics().IncrementDroppedMessages()

			// If there was an error routing the message, drop it
			msgIn.Destroy()
			continue
		}

		r.GetProcessorMetrics().IncrementProcessedMessages()
	}
}

func (r *routerRunner[T]) route(ctx context.Context, msg *msg[T]) error {
	ctx, span := r.Tel.StartTrace(msg.LoadSpanContext(ctx), "route message")
	defer span.End()

	id := r.routeFn(msg.GetBody())
	if err := r.checkRouteID(id); err != nil {
		r.Tel.LogError("failed to route message", err, "route_id", id)
		return err
	}

	routeName, outConnector, msgCount := r.getRoute(id)
	if err := outConnector.Write(msg); err != nil {
		r.Tel.LogError(
			"failed to write message to output connector", err,
			"route_id", id, "route_name", routeName,
		)

		return err
	}

	r.Metrics.IncrementTotalRoutedMessages()
	msgCount.Add(1)

	span.SetAttributes(
		attribute.Int("route_id", id),
		attribute.String("route_name", routeName),
	)

	return nil
}

func (r *routerRunner[T]) Close(_ context.Context) {
	<-r.runDone

	for _, outConnector := range r.outConnectors {
		outConnector.Close()
	}
}

func (r *routerRunner[T]) Inputs() []uintptr {
	return []uintptr{connector.GetConnectorID(r.inConnector)}
}

func (r *routerRunner[T]) Outputs() []uintptr {
	outputs := make([]uintptr, 0, len(r.outConnectors))

	for _, outConnector := range r.outConnectors {
		outputs = append(outputs, connector.GetConnectorID(outConnector))
	}

	return outputs
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// RouterStage is a processor stage that routes messages to different output
// connectors based on a user-defined function.
type RouterStage[T msgBody] struct {
	*stage.ProcessorStage[T, T, *routerEnv[T]]
}

// NewRouterStage returns a new router processor stage.
func NewRouterStage[T msgBody](routeFn RouterFn[T], inConnector msgConn[T]) *RouterStage[T] {
	env := newRouterEnv(routeFn)
	runner := newRouterRunner(inConnector)
	runner.SetEnvironment(env)

	return &RouterStage[T]{
		ProcessorStage: stage.NewProcessorStageFromRunner[T, T](
			"router", env, runner,
		),
	}
}

// AddRoute adds a new route to the stage.
// It returns the ID of the new route, which can be used in the routing function.
// The routes MUST be added before the stage is initialized, otherwise it will return an error.
func (s *RouterStage[T]) AddRoute(name string, outConnector msgConn[T]) int {
	return s.Env().addOutConnector(name, outConnector)
}
