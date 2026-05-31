# Stage Lifecycle

This document describes how stages are initialized, run, drained, and closed.
It focuses on the generic stage implementation under `internal/stage`.

The central shutdown rule is:

> Stop producers first, keep consumers alive while buffered and in-flight work
> drains, then close the queues owned by those consumers.

This rule matters because connector writes do not accept a context. A write to
a full queue blocks until another goroutine reads an item or closes the queue.

## Pipeline-Level Lifecycle

The pipeline builds a directed acyclic graph from the connectors returned by
each stage's `Inputs()` and `Outputs()` methods.

For a simple pipeline:

```text
Ingress A -> Processor B -> Processor C -> Egress D
```

the shutdown signal propagates in the same direction as messages:

```text
A.Run exits
  -> cancel B run context
  -> B.Run drains and exits
  -> cancel C run context
  -> C.Run drains and exits
  -> cancel D run context
  -> D.Run drains and exits
```

The relevant code is in `pipeline.go`:

1. Ingress stages receive the root run context.
2. Processor and egress stages receive a detached context with a dedicated
   cancel function.
3. A non-ingress stage is canceled only after all of its parents have exited
   `Run()`.
4. A stage publishes `runDoneCh` only after its own `Run()` method returns.

This means `Run()` is not merely the active processing phase. For a stage that
owns internal queues, it must include the drain phase. Returning from `Run()`
declares that downstream stages may begin shutting down.

`Pipeline.Close()` performs a separate resource cleanup pass. It cancels the
root run context, waits for each stage's `Run()` method to exit, and invokes the
stage's `Close()` method in topological order.

## Connector Semantics

The ring-buffer connector behavior is part of the lifecycle contract.

### Reads

`Read(ctx)` attempts to pop an item before checking whether the connector is
closed or the context is canceled. As a result:

- A closed connector can still be drained.
- A canceled context does not discard items that are already queued.
- `Read(ctx)` returns `connector.ErrClosed` only after a closed queue is empty.
- `Read(ctx)` returns `ctx.Err()` when an empty queue is waiting for data and
  the context is canceled.

When `Close()` is called on an empty ring buffer, it broadcasts to the
condition variable. This wakes a blocked `Read()` even when its context was
created with `context.WithoutCancel`. Queue closure can therefore be used as
the terminal signal for detached drain goroutines.

### Writes

`Write(item)` has no context parameter. As a result:

- A write to a full queue blocks until space is available.
- Closing the queue wakes blocked writers and causes them to return
  `connector.ErrClosed`.
- A canceled stage context does not unblock a blocked writer.

This asymmetry is the reason queue closure order must be explicit.

## Single Runner

`runnerSingle` creates one worker runner and connects it directly to the
external connectors.

```text
external input -> worker -> external output
```

Its lifecycle is:

```text
Init:
  create worker runner
  initialize worker

Run:
  repeatedly handle messages
  exit when a read returns context.Canceled or connector.ErrClosed

Close:
  close worker resources
  close the output connector, if present
```

There are no internal fan-out or fan-in queues in this mode.

## Pooled Runner

`runnerPool` adds internal queues and bridge goroutines around worker runners.

For a processor stage:

```text
external input
    |
    v
input bridge
    |
    v
internal fan-out queue (SPMC)
    |
    v
worker runners
    |
    v
internal fan-in queue (MPSC)
    |
    v
output bridge
    |
    v
external output
```

For an egress stage, there is no output queue or output bridge:

```text
external input -> input bridge -> internal fan-out queue -> worker runners
```

### Initialization

`runnerPool.Init()` initializes the scaler. The scaler creates one stop channel
per possible worker and queues startup requests for the initial workers.

### Active Run

`runnerPool.Run()` starts:

- The input bridge.
- The output bridge, for processor stages.
- The scaler loop.
- The worker-start listener.

The worker-start listener receives scaler requests, adds each worker goroutine
to `workerRunnerWg`, and starts its full lifecycle:

```text
worker Init -> worker RunPooled -> worker Close
```

`RunPooled()` checks the worker's scaler stop channel before starting each
message. It then handles one message from the internal fan-out queue. Its
processing context is detached from stage cancellation so workers continue
until the input bridge closes the fan-out queue.

### Drain Order

When the stage run context is canceled, a pooled processor must drain in this
ownership order:

```text
1. Stop the worker-start listener.
2. Let the input bridge finish.
3. The input bridge closes the internal fan-out queue.
4. Let workers drain the internal fan-out queue and finish in-flight tasks.
5. Wait for every worker goroutine.
6. Close the internal fan-in queue.
7. Let the output bridge drain the internal fan-in queue.
8. The output bridge closes the external output connector.
9. Close the scaler.
10. Return from Run().
```

An egress stage follows the same sequence without steps 6 through 8.

`runnerPool.Close()` waits for `Run()` to complete this sequence. Resource
cleanup for each pooled worker is invoked with `context.WithoutCancel(ctx)` so
that a canceled stage context does not prevent final flushes, such as a QuestDB
sender flush.

## Context Roles

Contexts have different roles and should not be treated as interchangeable.

| Context | Purpose | Cancellation behavior |
| --- | --- | --- |
| Pipeline root run context | Stops ingress stages | Canceled by the external signal or `Pipeline.Close()` |
| Non-ingress stage run context | Starts downstream drain after all parents stop | Canceled by the pipeline graph |
| Worker processing context | Drains the internal fan-out queue | Detached from stage cancellation; scaler stop channels and fan-out closure terminate pooled workers |
| Worker close context | Flushes and releases worker resources | Must not already be canceled |
| Pipeline close context | Bounds final resource cleanup | Supplied by the application |

`context.WithoutCancel` should be used deliberately. Detaching every operation
can prevent shutdown from terminating. Passing cancellation everywhere can
discard work or deadlock queues.

## Fan-In Safety Invariant

The output bridge for a pooled processor must remain alive until all workers
have stopped and the internal fan-in queue has been closed and drained.

The unsafe sequence is:

```text
stage context is canceled
  -> output bridge reads an empty fan-in queue
  -> output bridge exits because its Read(ctx) returns context.Canceled
  -> a worker finishes processing and writes to the fan-in queue
  -> enough workers fill the fan-in queue
  -> another worker blocks in Write()
  -> workerRunnerWg.Wait() waits forever
```

Therefore, using the cancelable stage run context directly for the output
bridge is unsafe unless another mechanism guarantees that the bridge cannot
exit before workers finish.

A robust output-bridge lifecycle uses queue closure as its terminal signal:

```text
workers finish
  -> runner closes internal fan-in queue
  -> output bridge drains remaining items
  -> output bridge observes connector.ErrClosed
  -> output bridge exits
```

`runnerPool.Run()` passes `context.WithoutCancel(ctx)` to `runOutput`. The
output bridge therefore remains alive after stage cancellation and exits only
after the runner closes the internal fan-in queue. This preserves backpressure
while workers finish their in-flight writes.

## Input Bridge Notes

The input bridge owns the internal fan-out queue:

```text
external input -> input bridge -> internal fan-out
```

When the input bridge exits, it closes the internal fan-out queue. Workers can
then consume queued messages and eventually observe `connector.ErrClosed`.

Workers must remain alive until that closure. If a pooled worker uses the
cancelable stage context directly, it can read a temporarily empty fan-out
queue after cancellation and exit while the input bridge is still publishing
buffered messages. Passing `context.WithoutCancel(ctx)` to `RunPooled` makes
fan-out closure the worker drain signal while scaler stop channels continue to
support scale-down.

For orderly pipeline shutdown, the parent stage closes the external input
connector after publishing all final output. Context cancellation is still
useful as a fallback for interrupting an empty read.

## Reorder Buffer Note

The CAN pipeline includes a custom single-threaded reorder-buffer runner. Its
input connector follows the same read semantics: already-buffered messages are
read before cancellation is returned. After the input is empty or closed, the
runner flushes its internal reorder buffer before exiting.

This is important because a stage can own state outside the connector queues.
Draining connectors alone is insufficient when a runner also buffers messages
internally.

## Checklist For Lifecycle Changes

Before changing shutdown logic, verify:

- Which goroutine owns each queue?
- Which goroutine closes each queue?
- Can any writer block after its reader exits?
- Does `Run()` return only after all output has been published?
- Does downstream cancellation happen only after upstream output closes?
- Can a canceled context interrupt an empty read while producers are still in
  flight?
- Are worker resource flushes given a usable close context?
- Does a custom runner buffer messages outside connector queues?
