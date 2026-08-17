---
icon: lucide/globe
---

# HTTP Ingress

`HTTPStage` runs an HTTP server and turns every accepted request into an
`HTTPMessage`. Unlike other ingress stages, HTTP is request-response: the
server keeps the client request open while the message travels through the
pipeline and waits for a correlated `egress.HTTPMessage` to return through a
shared `link.HTTP`.

The stage accepts every method and path. Routing, authentication,
authorization, and application behavior belong in downstream processors.

## Request-Response Model

The ingress and egress stages must share the same HTTP link. The link assigns a
correlation ID to each request and stores the future that will eventually hold
its response.

``` mermaid
flowchart LR
  client[HTTP Client]
  ingress[HTTP Ingress]
  request[Request Connector]
  processor[Processor]
  response[Response Connector]
  egress[HTTP Egress]
  link[Shared HTTP Link]

  client -->|HTTP Request| ingress
  ingress -->|HTTPRequest| request
  request --> processor
  processor -->|HTTPResponse| response
  response --> egress
  ingress <-->|Correlated Future| link
  egress -->|Resolve Or Reject| link
  ingress -->|HTTP Response| client
```

A minimal pipeline has all three stages:

``` go
requestConnector := connector.NewRingBuffer[*ingress.HTTPMessage](512)
responseConnector := connector.NewRingBuffer[*egress.HTTPMessage](512)

httpLink := link.NewHTTP()

ingressCfg := ingress.NewHTTPConfig()
httpIngress := ingress.NewHTTPStage(httpLink, requestConnector, ingressCfg)

processorCfg := processor.NewGenericConfig(goccia.StageRunningModeSingle)
processorCfg.Name = "http_handler"
handler := processor.NewGenericStage(
    newHTTPHandler(),
    requestConnector,
    responseConnector,
    processorCfg,
)

egressCfg := egress.NewHTTPConfig()
httpEgress := egress.NewHTTPStage(httpLink, responseConnector, egressCfg)

pipeline := goccia.NewPipeline()
pipeline.AddStage(httpIngress)
pipeline.AddStage(handler)
pipeline.AddStage(httpEgress)
```

The generic handler returns the response body consumed by HTTP egress:

``` go
type httpHandler struct {
    processor.GenericHandlerBase
}

func newHTTPHandler() *httpHandler {
    return &httpHandler{}
}

func (h *httpHandler) Handle(
    _ context.Context,
    req *ingress.HTTPMessage,
) (*egress.HTTPMessage, error) {
    return &egress.HTTPMessage{
        StatusCode: http.StatusOK,
        Header: http.Header{
            "Content-Type": []string{"application/octet-stream"},
        },
        Body: req.Body,
    }, nil
}
```

Generic processor stages preserve the request envelope's correlation ID on the
response envelope. A custom processor must preserve that metadata itself if it
constructs or forwards envelopes outside the standard worker path.

## Messages

### Output Message

Produced body type: `*ingress.HTTPMessage`, an alias of the internal
`*message.HTTPRequest` type.

| Field | Description |
| --- | --- |
| `RemoteAddr` | Client network address reported by `net/http`, normally in `IP:port` form. |
| `Method` | HTTP request method exactly as received, such as `GET`, `POST`, or `PATCH`. |
| `Path` | Decoded URL path from `req.URL.Path`. |
| `Query` | Raw query string from `req.URL.RawQuery`, without the leading `?`. |
| `Header` | A clone of the complete request header map. |
| `Body` | Entire request body buffered in memory as a byte slice. |

Additional interfaces: none. `HTTPMessage` does not implement
`message.Serializable`.

The stage sets both the envelope receive time and timestamp to the instant the
HTTP handler starts. It also assigns the correlation ID used by `link.HTTP`.
Destroying the final envelope clears the request fields and releases its
references to the header and body.

The message does not contain the URL scheme, host, protocol version, request
trailers, or the original `*http.Request`. If application logic needs one of
those values, copy it into an application-specific message in a custom ingress
stage or encode it in a trusted header before the request reaches Goccia.

### Response Message

The corresponding response body type is `*egress.HTTPMessage`, an alias of
`*message.HTTPResponse`:

| Field | Description |
| --- | --- |
| `StatusCode` | HTTP response status. The companion HTTP egress accepts values from `200` through `599`. |
| `Header` | Headers added to the server response before the status is written. |
| `Body` | Response bytes written after the status and headers. |

HTTP egress reads the response envelope's correlation ID and resolves the
matching future. Responses may therefore complete out of order. If the future
has already timed out or been canceled, HTTP egress destroys the late response
instead of leaking it.

## Configuration

Create a complete default configuration with `ingress.NewHTTPConfig()`:

``` go
cfg := ingress.NewHTTPConfig()
cfg.IPAddr = "127.0.0.1"
cfg.Port = 8080

stage := ingress.NewHTTPStage(httpLink, requestConnector, cfg)
```

| Field | Default | Description |
| --- | --- | --- |
| `IPAddr` | `"0.0.0.0"` | Local address passed to `net.JoinHostPort` for the server listener. The default listens on all IPv4 interfaces. |
| `Port` | `8080` | Local TCP port. |
| `ReadTimeout` | `10 * time.Second` | Maximum duration for reading an HTTP request, including its body, as defined by `http.Server`. |
| `ReadHeaderTimeout` | `5 * time.Second` | Maximum duration for reading request headers. |
| `ShutdownTimeout` | `10 * time.Second` | Grace period used by `http.Server.Shutdown` when the stage stops. |
| `IdleTimeout` | `60 * time.Second` | Maximum idle time between requests on a keep-alive connection. |
| `MaxRequestBodySize` | `4 << 20` | Maximum buffered request body size: 4 MiB by default. |
| `ResponseTimeout` | `10 * time.Second` | Maximum wait for the correlated downstream response after the request enters the internal queue. |
| `WriteTimeout` | `10 * time.Second` | Maximum duration for writing the HTTP response, as defined by `http.Server`. |
| `OutputQueueSize` | `512` | Capacity of the internal multi-producer queue between concurrent HTTP handlers and the output connector. |
| `TLSEnabled` | `false` | Serve HTTPS when enabled. |
| `TLSConfig` | `nil` | TLS configuration containing either a certificate or a `GetCertificate` callback. |

An empty address, a zero port, non-positive durations, a non-positive body
limit, and a non-positive queue size are configuration anomalies. During stage
initialization, Goccia logs each anomaly and replaces the invalid value with
its default.

`link.HTTP`, the output connector, and the configuration must be non-nil and
must remain valid for the lifetime of the stage.

## TLS

To serve HTTPS, enable TLS and provide a certificate source:

``` go
certificate, err := tls.LoadX509KeyPair("server.crt", "server.key")
if err != nil {
    return err
}

cfg := ingress.NewHTTPConfig()
cfg.TLSEnabled = true
cfg.TLSConfig = &tls.Config{
    Certificates: []tls.Certificate{certificate},
}
```

At initialization, the stage:

1. Requires `TLSConfig` when TLS is enabled.
2. Requires at least one certificate or a `GetCertificate` callback.
3. Clones the supplied `tls.Config`, so it does not mutate the caller's value.
4. Sets the clone's minimum TLS version to TLS 1.2 when `MinVersion` is zero.

An explicitly configured `MinVersion` is preserved. Certificate and key file
loading is the caller's responsibility; the stage starts `ListenAndServeTLS`
with the certificates already present in the cloned configuration.

## Response and Error Behavior

| Condition | Client Result | Pipeline Result |
| --- | --- | --- |
| Request body exceeds `MaxRequestBodySize`, or body reading fails | `413 Request Entity Too Large` with `request body too large` | No request message is emitted. |
| Internal request queue is closed before enqueue | `503 Service Unavailable` with `service unavailable` | The future is rejected and the request message is destroyed. |
| HTTP egress rejects the future, including an invalid response status | `502 Bad Gateway` with `bad gateway` | The rejected response message is destroyed by HTTP egress. |
| Future resolves without a response message | `502 Bad Gateway` with `bad gateway` | No response body is written. |
| `ResponseTimeout` expires | `504 Gateway Timeout` with `gateway timeout` | The pending future is removed; a response that won the completion race is collected and destroyed. |
| Client request context is canceled | No additional response is attempted | The pending future is removed and any concurrently completed response is destroyed. |
| Future resolves normally | Response status, headers, and body come from `egress.HTTPMessage` | HTTP ingress destroys the resolved response after writing it. |

Error responses are produced with `http.Error`, so Go adds its normal plain-text
response headers and a trailing newline to the message text.

HTTP egress validates response codes before resolving a future. The accepted
range deliberately excludes informational `1xx` responses and invalid values.
Application handlers should normally return a code from `200` through `599`.

## Concurrency and Backpressure

Go's HTTP server invokes the handler concurrently for independent requests.
Those handler goroutines write to an internal MPSC ring buffer whose capacity
is `OutputQueueSize`; a single bridge goroutine forwards messages to the
configured output connector.

If the internal queue is full, request handlers wait for space. If the output
connector is backpressured, the bridge waits there and the internal queue can
fill in turn. The response timeout starts once a request is accepted by the
internal queue, so it includes time spent waiting for the bridge, downstream
connectors, processors, and HTTP egress.

Every accepted request also occupies one future until it resolves, rejects,
times out, or is canceled. The future registry is sharded for concurrent
access, and correlation IDs increase monotonically within a link instance.

Processors may run in pool mode and responses may arrive in a different order
from requests. Correlation IDs, rather than connector ordering, pair each
response with its client.

## Shutdown

When the stage run context is canceled, HTTP ingress first calls
`http.Server.Shutdown` with `ShutdownTimeout`. Graceful shutdown stops accepting
new connections and waits for active handlers. If the grace period expires,
the stage logs the failure and force-closes the server.

After the HTTP server returns, the stage closes and drains its internal queue,
lets the bridge finish forwarding accepted messages, and closes the output
connector. This ordering allows downstream stages to finish requests already
accepted by the server during normal pipeline shutdown.

An unexpected server failure is logged and also ends the ingress runner. Bind
errors, including an address already in use, occur when the server starts in
`Run`, not during `Pipeline.Init`.

## Metrics

--8<-- "ingress/metrics/docs/http_stage.metrics.doc.md"

### Attributes

--8<-- "ingress/metrics/docs/http_stage.attributes.doc.md"

`goccia.http.ingress.queue.wait.duration` has an `outcome` of `enqueued` or
`rejected`. `goccia.http.ingress.response.wait.duration` uses the future state:
`resolved`, `rejected`, `timeout`, or `canceled`.

Durations are recorded in seconds, body sizes in bytes, active and pending
requests in `{request}`, and queue length in `{message}`. Request-body size
includes bytes observed before an oversized or interrupted read failed.
Response-body size counts successfully written bytes; it is explicitly
recorded as zero for `HEAD` requests.

## Tracing

HTTP ingress extracts remote trace context from the request headers using the
globally configured OpenTelemetry text-map propagator. When the headers contain
valid context understood by that propagator, the HTTP server span continues the
remote trace; otherwise, it starts a new trace.

The stage creates one server span for each request. Its name is the HTTP request
method, its span kind is `server`, and it remains open until request processing
and response writing finish. In addition to the common `stage.kind` and
`stage.name` attributes, the span records:

| Attribute | Value |
| --- | --- |
| `http.request.method` | Request method exactly as received. |
| `url.path` | Decoded path from `req.URL.Path`. |
| `url.scheme` | `http` or `https`, inferred from the request's TLS state. |
| `network.protocol.version` | Protocol version reported by `net/http`. |
| `http.response.status_code` | Status recorded by the response writer after request processing. |

Before enqueueing the request, HTTP ingress saves the server span context on the
message envelope. Processor and egress stages load that context before creating
their spans, so the pipeline remains part of the same trace. The HTTP egress
response-handling span is therefore a descendant of the ingress server span.

The stage does not inject trace context into response headers. Which propagation
formats are extracted, such as W3C Trace Context and Baggage, depends on the
application's global OpenTelemetry propagator configuration.

## Operational Notes

- The default `0.0.0.0:8080` listener is externally reachable wherever host
  networking and firewall rules permit. Bind to a loopback or private address
  when public access is not intended.
- Use TLS for traffic that crosses an untrusted network. Goccia does not create,
  rotate, or reload certificates automatically.
- Enforce authentication and authorization in a downstream processor or in a
  trusted reverse proxy. The ingress stage itself accepts all routes.
- Keep `ResponseTimeout` aligned with downstream latency and `WriteTimeout`.
  A response produced after the future is removed is discarded safely, but its
  processing work has already been spent.
- Set `MaxRequestBodySize` before increasing concurrency. Body memory is paid
  per active request, not once per stage.

## Internals

The stage wraps Go's `net/http.Server` in a custom ingress runner. The server's
handler goroutines feed an internal MPSC ring buffer, and a detached bridge
goroutine serializes those requests into the output connector. A shared,
sharded future registry in `link.HTTP` connects the ingress handler to the HTTP
egress runner without requiring responses to preserve request order.

See the repository's
[`examples/http`](https://github.com/FerroO2000/goccia/tree/master/examples/http)
application for a complete echo server.
