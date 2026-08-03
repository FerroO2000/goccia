---
icon: lucide/route
---

# Router Processor

`RouterStage` forwards each input message to exactly one output connector based
on a user-defined routing function.

``` go
var evenRouteID int
var oddRouteID int

stage := processor.NewRouterStage(func(msg *ingress.TickerMessage) int {
	if msg.TickNumber%2 == 0 {
		return evenRouteID
	}

	return oddRouteID
}, in)

evenRouteID = stage.AddRoute("even", evenOut)
oddRouteID = stage.AddRoute("odd", oddOut)
```

## Messages

### Input Message

Accepted body type: `T`.

Additional input interfaces: none required beyond `message.Body`. The route
function receives the message body and returns the integer route ID selected
for that message.

### Output Message

Produced body type: the same `T` body received from the input connector.

Additional interfaces: preserved from `T`. The stage does not clone, copy, or
transform the message. It forwards the original message envelope to the selected
output connector.

## Configuration

`RouterStage` has no config object. It is configured through the constructor
and route registration methods:

| API | Description |
| --- | --- |
| `processor.NewRouterStage(routeFn, in)` | Creates the router with an input connector and a `RouterFn[T]`. |
| `stage.AddRoute(name, out)` | Adds an output route and returns the route ID used by `routeFn`. |

At least one route is required. Routes must be added before the stage is
initialized.

The route function must return a non-negative integer less than the number of
registered routes. If it returns an invalid route ID, the message is dropped and
the processor error and dropped-message metrics are incremented.

## Metrics

--8<-- "processor/metrics/router_stage.doc.md"

`RouterStage` also records `routed_messages_per_route`, a per-route message
count with `route_id` and `route_name` attributes.

## Internals

`RouterStage` uses a custom single runner instead of the generic worker pool.
The runner reads one message, calls the route function, validates the returned
route ID, writes the message to the selected output connector, and closes every
output connector when the input stream drains.

Because the stage forwards the original message envelope, each input message is
owned by exactly one downstream branch after routing. Failed writes and invalid
routes destroy the message during the router error path.
