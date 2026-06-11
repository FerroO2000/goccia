---
icon: lucide/link
---

# Connectors

Connectors join stages together. They transport messages, define the edges of
the pipeline graph, and apply backpressure when consumers cannot keep up with
producers. This page builds on the [pipeline graph](pipeline.md#stage-graph)
introduced in the previous concept guide.

``` mermaid
flowchart LR
  ingress[Ingress stage] -->|connector| processor[Processor stage]
  processor -->|connector| egress[Egress stage]
```

## Create a Connector

Goccia includes a bounded ring-buffer connector:

``` go
const connectorSize = 512

tickerToCustom := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)
customToSink := connector.NewRingBuffer[*ingress.TickerMessage](connectorSize)
```

Pass the same connector instance to the producing stage and the consuming
stage:

``` go
tickerStage := ingress.NewTickerStage(tickerToCustom, tickerCfg)
customStage := processor.NewGenericStage(
    handler,
    tickerToCustom,
    customToSink,
    customCfg,
)
sinkStage := egress.NewSinkStage(customToSink)
```

The generic type is the message body carried across the edge. Internally,
Goccia wraps each body in a message envelope that stores lifecycle and
telemetry metadata.

## Bounded Queues and Backpressure

The built-in connector has a fixed capacity. A producer can continue writing
while space is available. When the queue fills up, the next `Write` waits until
a consumer reads a message:

``` text
producer faster than consumer
-> connector reaches capacity
-> producer waits
-> pressure propagates upstream
```

This behavior bounds memory usage and avoids silently dropping messages.

!!! tip

    Start with a moderate capacity, such as `512`, and tune it using your
    workload metrics. A larger queue can absorb bursts, but it also retains
    more messages during shutdown and can increase end-to-end latency.

## Built-In Lock-Free Ring Buffer

The built-in connector is optimized for stage-to-stage message passing. Its
storage is a fixed-size circular array: after reaching the end of the array,
the next write wraps back to the beginning and reuses slots that consumers have
already released.

``` mermaid
flowchart LR
  producer[Producer] --> head[Head index]
  head --> slots[Fixed-size ring]
  slots --> tail[Tail index]
  tail --> consumer[Consumer]
```

The public `connector.NewRingBuffer` constructor creates an **SPSC**
(single-producer, single-consumer) buffer. This matches a normal pipeline edge:
one stage writes messages and the following stage reads them.

Goccia also uses specialized variants internally for pooled stages:

| Variant | Producers | Consumers | Used for |
| --- | --- | --- | --- |
| SPSC | One | One | Public connectors between stages |
| SPMC | One | Many | Internal fan-out from an input bridge to pooled workers |
| MPSC | Many | One | Internal fan-in from pooled workers to an output bridge |

### Fast Path

When the ring has space or queued data, reads and writes use atomic operations
instead of taking a mutex:

- The producer advances a `head` index.
- The consumer advances a `tail` index.
- An index mask maps those counters onto slots in the circular array.
- Cache-line padding separates frequently updated fields to reduce contention.

The requested capacity is rounded up to a power of two. This allows Goccia to
map an index to a slot using a bit mask instead of a remainder operation. For
example, requesting capacity `500` creates a ring with capacity `512`.

### Waiting Path

Lock-free does not mean busy-waiting forever. When a queue remains empty or
full after a short spin period, the connector waits on a condition variable:

``` text
optimistic atomic operation
-> briefly yield while another goroutine may make progress
-> wait efficiently if the queue is still empty or full
-> wake when data, capacity, cancellation, or closure changes the state
```

This hybrid approach keeps the common path lightweight while avoiding needless
CPU usage during longer idle or backpressure periods.

## Read, Write, and Close

Every connector implements:

``` go
type Connector[T any] interface {
    Write(item T) error
    Read(ctx context.Context) (T, error)
    Close()
}
```

### Write

`Write(item)` adds one message to the queue.

- It waits when the queue is full.
- It returns `connector.ErrClosed` if the connector has been closed.
- It does not accept a context, so cancellation alone does not interrupt a
  blocked write.

### Read

`Read(ctx)` removes one message from the queue.

- It waits when the queue is empty.
- It returns `ctx.Err()` if the context is canceled while waiting.
- It returns `connector.ErrClosed` when the connector is closed and empty.
- It continues returning buffered messages after `Close()` until the queue has
  been drained.

### Close

`Close()` marks the connector as closed and wakes blocked readers and writers.
It is safe to call more than once.

During normal pipeline usage, stages close their output connectors as part of
the coordinated shutdown sequence. Application code generally calls
`Pipeline.Close()` rather than closing connectors individually.

## Shutdown Example

If a connector contains two messages when its producer closes it, the consumer
can still drain both messages:

``` text
producer closes connector
-> consumer reads queued message 1
-> consumer reads queued message 2
-> consumer Read returns connector.ErrClosed
-> consumer stage exits
```

This drain-before-exit behavior is essential to Goccia's graceful shutdown
model.

## Custom Connectors

The public `connector.Connector[T]` interface can be implemented by custom
connectors. A custom implementation should preserve the same lifecycle
contract:

- `Read(ctx)` must return when its context is canceled while waiting.
- `Close()` must wake blocked readers and writers.
- Buffered messages should remain readable after `Close()`.
- Writes after `Close()` must return `connector.ErrClosed`.
- Each connector instance must have stable pointer identity so Goccia can use
  it to build the stage graph.

Use the built-in ring buffer unless you need transport-specific behavior or a
different queueing strategy.

Next, read about the [stages](stages.md) that produce and consume messages
through these connectors.
