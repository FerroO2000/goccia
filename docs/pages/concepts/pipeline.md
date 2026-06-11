---
icon: lucide/git-branch
---

# Pipeline Concepts

The pipeline coordinates a graph of [stages](stages.md). Its most important
responsibility is graceful shutdown: upstream stages stop producing first,
while downstream stages remain alive long enough to consume every queued and
in-flight message.

## Stage Graph

Goccia models a pipeline as a directed acyclic graph:

``` mermaid
flowchart LR
  A[Ingress] --> B[Processor]
  B --> C[Processor]
  C --> D[Egress]
```

A [connector](connectors.md) identifies each edge. Each node is a
[stage](stages.md). The graph determines initialization order and shutdown
propagation.

### Building the Graph

When `Pipeline.Init()` runs, the pipeline builds the graph from connector
identity:

``` text
stage output connector ID
-> matching downstream input connector ID
-> parent-child edge
```

A stage with no parent is an ingress node. The graph can have more than one
ingress node, fan-out branches, and fan-in joins:

``` mermaid
flowchart LR
  A[Ingress A] --> C[Processor C]
  B[Ingress B] --> D[Processor D]
  C --> E[Processor E]
  D --> E
  E --> F[Egress F]
```

The pipeline traverses this graph in topological order: every parent appears
before its children. Initialization and final cleanup use this order.

!!! note

    Add stages in upstream-to-downstream order. Graph construction resolves
    each input by looking up an output connector that has already been
    registered.

## Pipeline Lifecycle

A pipeline moves through four phases:

``` mermaid
flowchart LR
  idle[Idle] -->|AddStage| idle
  idle -->|Init| initialized[Initialized]
  initialized -->|Run| running[Running]
  running -->|Close| closed[Closed]
```

The typical application flow is:

``` go
pipeline := goccia.NewPipeline()
pipeline.AddStage(source)
pipeline.AddStage(processor)
pipeline.AddStage(destination)

if err := pipeline.Init(ctx); err != nil {
    return err
}

go pipeline.Run(ctx)

<-ctx.Done()
pipeline.Close(closeCtx)
```

`Run()` blocks until every stage has returned from its own `Run()` method.
Applications commonly start it in a goroutine and call `Close()` after
receiving a shutdown signal.

### Initialization

`Pipeline.Init(ctx)`:

1. Builds the stage graph.
2. Traverses stages from upstream to downstream.
3. Calls `Init(ctx)` on each stage.

Each stage initializes its environment before its runner. This is where
configuration validation, metrics setup, and stage-specific resources are
created.

### Running

`Pipeline.Run(ctx)` starts one run goroutine for every stage. Processor and
egress stages also receive a small watcher goroutine that controls when their
run context is canceled.

All stage run goroutines start together. Shutdown order is enforced later by
context cancellation and `runDoneCh`, not by delaying the startup of
downstream stages.

## Backpressure

[Connectors](connectors.md) are bounded queues. When a queue is full, its
producer waits until a consumer reads a message. This bounds memory usage and
propagates pressure upstream instead of silently dropping work.

Backpressure is why shutdown cannot cancel every stage at once. If an upstream
stage is blocked in `Write()`, its downstream consumer must remain alive until
the write completes or the connector closes.

## Context Cancellation Chain

Shutdown cancellation propagates through the graph in the same direction as
messages:

``` text
ingress stops
-> its Run returns
-> direct children are canceled
-> children drain and return
-> their children are canceled
-> repeat until every leaf has stopped
```

This is not Go's ordinary parent-context propagation. The pipeline constructs
the cancellation chain deliberately.

### Context Roles

`Pipeline.Run(ctx)` creates two context families:

| Context | Used by | Cancellation behavior |
| --- | --- | --- |
| Root run context | Ingress stages | Derived from the caller's `ctx`; canceled by the caller or `Pipeline.Close()` |
| Detached stage context | Processor and egress stages | Detached from the caller's cancellation, then wrapped with a dedicated cancel function |

In simplified form:

``` go
runRootCtx, cancelRunRootCtx := context.WithCancel(ctx)
detachedCtx := context.WithoutCancel(ctx)

for each stage {
    runCtx := runRootCtx

    if stage is not ingress {
        runCtx, stage.cancelRunCtx = context.WithCancel(detachedCtx)
    }

    go runStage(runCtx, stage)
}
```

The detached downstream contexts are essential. When the external signal
cancels `ctx`, ingress stages should stop reading from external sources.
Processors and egress stages must stay alive because ingress stages may still
be finishing blocked writes or publishing final messages.

### Per-Stage Completion Signal

Every graph node owns a `runDoneCh`. A stage closes this channel only after its
`Run()` method has returned:

``` text
stage.Run(ctx)
-> stage drains its local work
-> stage.Run returns
-> close stage.runDoneCh
```

For a non-ingress stage, the pipeline starts a watcher:

``` go
for parent := range stage.parents() {
    <-parent.runDoneCh
}

stage.cancelRun()
```

The watcher waits for every immediate parent. Only then does it cancel the
stage's dedicated run context.

### Linear Example

Consider a simple pipeline:

``` mermaid
flowchart LR
  A[Ingress A] --> B[Processor B]
  B --> C[Egress C]
```

When the application receives a shutdown signal:

``` text
external context is canceled
-> A run context is canceled
-> A stops reading its external source
-> A finishes any current output write
-> A.Run returns and closes A.runDoneCh
-> B watcher cancels B run context
-> B drains queued and in-flight messages
-> B.Run returns and closes B.runDoneCh
-> C watcher cancels C run context
-> C drains final deliveries
-> C.Run returns and closes C.runDoneCh
```

At each step, the downstream consumer stays active until its upstream producer
has finished.

### Fan-In Example

A fan-in stage must not stop when only one parent finishes:

``` mermaid
flowchart LR
  A[Ingress A] --> C[Processor C]
  B[Ingress B] --> C
  C --> D[Egress D]
```

The cancellation chain is:

``` text
A.Run returns ----\
                   -> wait for both parents -> cancel C
B.Run returns ----/

C.Run drains and returns
-> cancel D
```

If `A` finishes before `B`, `C` remains active so it can continue consuming
messages from `B`. The same rule applies at every join in a larger graph.

### Fan-Out Example

When one parent has multiple children, each child has its own watcher and
dedicated run context:

``` mermaid
flowchart LR
  A[Processor A] --> B[Egress B]
  A --> C[Egress C]
```

After `A.Run()` returns, both `B` and `C` can begin draining independently.
One branch does not wait for the other branch unless they meet again at a
downstream fan-in node.

## Graceful Shutdown

The pipeline-level cancellation wave composes with each stage's local drain
rules:

``` text
stop upstream producer
-> let producer publish final output
-> cancel direct downstream consumers
-> let consumers drain queued and in-flight work
-> continue downstream
```

For a [pooled processor stage](stages.md#pooled-execution-mode), the local
drain adds internal fan-out and fan-in queues:

``` mermaid
flowchart LR
  input[External input] --> inbridge[Input bridge]
  inbridge --> fanout[Fan-out queue]
  fanout --> workers[Worker executors]
  workers --> fanin[Fan-in queue]
  fanin --> outbridge[Output bridge]
  outbridge --> output[External output]
```

After the stage run context is canceled:

``` text
input bridge stops and closes fan-out
-> worker executors drain fan-out and finish in-flight work
-> runner waits for every executor
-> runner closes fan-in
-> output bridge drains fan-in
-> output bridge closes the external output connector
-> stage Run returns
```

This ordering keeps downstream consumers alive while producers can still be
blocked by backpressure.

## Closing Resources

`Pipeline.Close(ctx)` starts the shutdown if it has not already begun:

``` text
cancel root run context
-> wait for each stage Run to return
-> call each stage Close(ctx) in topological order
```

The root cancellation affects ingress stages directly. The downstream
cancellation chain then proceeds through `runDoneCh` as described above.

`Run()` and `Close()` have different responsibilities:

| Method | Responsibility |
| --- | --- |
| `Run()` | Process messages and complete the graceful drain |
| `Close(ctx)` | Release resources after draining has completed |

For example, an ingress runner closes its output connector during cleanup, and
an egress environment may flush or close an external client.

The context passed to `Pipeline.Close(ctx)` is the application's cleanup
context. It is separate from the canceled run context so resource cleanup can
still perform final operations. Applications can give it a deadline:

``` go
closeCtx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
defer cancelClose()

pipeline.Close(closeCtx)
```

!!! important

    Canceling the run context stops and drains stage execution. Call
    `Pipeline.Close()` as well so the pipeline waits for that drain and releases
    stage resources.

Next, read about the [connectors](connectors.md) that define graph edges and
carry messages between stages.
