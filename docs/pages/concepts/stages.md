---
icon: lucide/boxes
---

# Stages

Stages are the active nodes of a Goccia pipeline. A stage reads from zero or
more input [connectors](connectors.md), performs work, and writes to zero or
more output connectors. This page builds on the pipeline lifecycle and
connector behavior introduced in the previous concept guides.

## The Base Stage

Every stage kind embeds a `BaseStage`. This common shell exposes the stage to
the pipeline and delegates execution to two main components: a runner and an
environment.

``` go
type BaseStage[Env env.Env] struct {
    kind Kind
    name string

    env Env
    runner Runner[Env]
}
```

``` mermaid
flowchart LR
  stage[BaseStage] --> runner[Runner]
  stage --> env[Environment]
  env --> config[Configuration]
  env --> telemetry[Telemetry and Metrics]
```

`BaseStage` also stores the stage kind and name used by the pipeline and
telemetry. Its lifecycle methods call the environment and runner in ownership
order:

``` text
Init:
  initialize environment
  attach environment to runner
  initialize runner

Run:
  run runner

Close:
  close runner
  close environment
```

### Environment

The environment is shared stage state. It is available to the runner and, for
worker-backed stages, to every worker executor. A typical environment contains:

- The stage configuration.
- Telemetry and metrics.
- Resources shared across the stage, such as a network connections.

`BaseStage` creates telemetry from the stage kind and name, then attaches it to
the environment before initialization. The environment uses that telemetry for
logs, traces, and metrics.

The base environment validates its configuration and initializes metrics in
`Init()`. Individual stage environments can extend that lifecycle to create
resources from their configuration, and release those resources in `Close()`.

### Runner

A stage runner implements:

``` go
type Runner[Env env.Env] interface {
    SetEnvironment(env Env)
    Init(ctx context.Context) error
    Run(ctx context.Context)
    Close(ctx context.Context)
    Inputs() []uintptr
    Outputs() []uintptr
}
```

The runner owns active execution. Its `Inputs()` and `Outputs()` methods expose
connector IDs so the pipeline can discover graph edges. Its remaining methods
define how work is initialized, run, drained, and closed.

A stage can provide a custom runner when it needs source-specific behavior or
special lifecycle rules. Processor and egress stages can also use the generic
single or pooled runners described later on this page.


## Stage Kinds

The common `BaseStage` shell is embedded by three stage variants:

| Kind | Inputs | Outputs | Purpose |
| --- | --- | --- | --- |
| Ingress | None | One or more | Produces messages from an external source |
| Processor | One or more | One or more | Transforms, filters, or routes messages |
| Egress | One or more | None | Delivers messages to an external destination |

## Ingress Stages

Ingress stages are the simplest variant. They do not receive messages from
another stage, and they do not use the generic worker-pool machinery. Instead,
an ingress stage implements a custom runner directly:

``` mermaid
flowchart LR
  source[External source] --> runner[Ingress runner]
  runner --> output[Output connector]
```

For example, the ticker ingress runner waits for a timer, creates a message,
and writes it to its output connector:

``` go
func (tr *tickerRunner) Run(ctx context.Context) {
    defer close(tr.runDone)

    ticker := time.NewTicker(tr.Config.Interval)
    defer ticker.Stop()

    ticks := 0
    for {
        ticks++

        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            msgOut := tr.handleTrigger(ctx, ticks)
            if err := tr.outConnector.Write(msgOut); err != nil {
                msgOut.Destroy()
            }
        }
    }
}
```

An ingress runner owns the details of its source.

The generic ingress stage wraps the runner and environment in `BaseStage`:

``` go
stage.NewIngressStageFromRunner(
    "ticker",
    newTickerEnv(cfg),
    newTickerRunner(output),
)
```

Specialized processor and egress stages can use the same custom-runner path.
For example, a reorder buffer can own state and flushing rules that do not fit
a one-message-at-a-time worker.

## Worker-Backed Stages

Most processor and egress stages can be expressed as one operation applied to
each incoming message. Goccia builds these stages from workers:

``` mermaid
flowchart TB
  stage[BaseStage] --> runner[Runner]
  stage --> env[Environment]
  env --> config[Configuration]
  env --> telemetry[Telemetry and metrics]

  runner --> backend[Runner backend]
  backend --> executor[Worker executor]
  executor --> handler[Worker handler]
  handler --> worker[Worker]

  backend -. uses .-> env
  executor -. uses .-> env
  worker -. receives .-> env
  handler -. records .-> telemetry

  classDef base fill:#dbeafe,stroke:#2563eb,color:#172554
  classDef worker fill:#ffedd5,stroke:#ea580c,color:#431407

  class stage,runner,env,config,telemetry base
  class backend,executor,handler,worker worker
```

Each layer has a distinct responsibility:

| Component | Responsibility |
| --- | --- |
| Stage | Exposes the stage lifecycle and graph identity |
| Runner | Chooses single or pooled execution and coordinates shutdown |
| Runner backend | Adapts processor or egress behavior to the generic runner |
| Worker executor | Repeatedly executes one worker instance |
| Worker handler | Performs the per-message bookkeeping around the worker |
| Worker | Contains the stage-specific processing or delivery logic |
| Environment | Stores shared configuration, telemetry, metrics, and resources |

### Runner Backend

The runner backend is the adapter between generic stage orchestration and the
kind-specific worker path:

``` go
type stageRunnerBackend[Env env.Env, W worker.Worker[Env]] interface {
    setEnvironment(env Env)
    newWorkerExecutor(workerID int) *worker.Executor[Env, W]
    runInputBridge(ctx context.Context)
    runOutputBridge(ctx context.Context)
    closeOutput()
    inputConnectorID() uintptr
    outputConnectorID() uintptr
}
```

A processor backend has an input and an output. It can create both an input
reader and an output writer for each worker executor.

An egress backend has only an input. Its output bridge and output close
operations are intentionally empty because delivery terminates outside the
pipeline.

### Worker Executor

A worker executor owns one worker instance and repeatedly invokes its handler:

``` text
executor
-> read one message
-> invoke worker-specific logic
-> publish or deliver the result
-> repeat
```

The executor has two execution methods:

| Method | Used by | Stops when |
| --- | --- | --- |
| `Run(ctx)` | Single runner | Input read observes cancellation or connector closure |
| `RunPooled(ctx, stopCh, pendingCounter)` | Pooled runner | Internal input closes or the scaler requests scale-down |

Each pooled executor receives its own worker instance. The environment is
shared by all executors in the stage.

### Processor Handler

A processor worker implements:

``` go
type Processor[Env env.Env, In, Out msgBody] interface {
    Worker[Env]
    Handle(ctx context.Context, msgIn *msg[In]) (*msg[Out], error)
}
```

The processor handler wraps each call to `Handle` with common behavior:

1. Read an input message.
2. Restore its tracing context.
3. Invoke the worker.
4. Record processing errors and metrics.
5. Propagate timestamp metadata to the output message.
6. Destroy dropped messages or write produced messages downstream.
7. Destroy the consumed input message.

### Egress Handler

An egress worker implements:

``` go
type Egress[Env env.Env, In msgBody] interface {
    Worker[Env]
    Deliver(ctx context.Context, msgIn *msg[In]) error
}
```

The egress handler reads a message, restores its tracing context, invokes
`Deliver`, records metrics, and destroys the consumed message. There is no
output connector.

## Single Execution Mode

Single mode creates one worker executor and attaches it directly to the
external connectors:

``` mermaid
flowchart LR
  input[External input] --> executor[Worker executor]
  executor --> output[External output]
```

The stage does not allocate internal queues or bridge goroutines. The external
connector provides buffering and backpressure.

Use single mode when:

- Message ordering must be preserved.
- Processing is lightweight.
- The external dependency should receive requests serially.
- A stage-specific resource is not safe to use concurrently.

## Pooled Execution Mode

Pool mode adds internal queues and bridges around multiple worker executors:

``` mermaid
flowchart LR
  input[External input] --> inbridge[Input bridge]
  inbridge --> fanout[Fan-out queue SPMC]
  fanout --> executors[Worker executors]
  executors --> fanin[Fan-in queue MPSC]
  fanin --> outbridge[Output bridge]
  outbridge --> output[External output]
```

The internal queue variants match their traffic:

| Queue | Producers | Consumers | Purpose |
| --- | --- | --- | --- |
| Fan-out | One input bridge | Many worker executors | Distribute incoming work |
| Fan-in | Many worker executors | One output bridge | Gather processed messages |

An egress pool does not need a fan-in queue or output bridge:

``` mermaid
flowchart LR
  input[External input] --> inbridge[Input bridge]
  inbridge --> fanout[Fan-out queue SPMC]
  fanout --> executors[Worker executors]
  executors --> destination[External destination]
```

### Input Bridge

The input bridge reads from the external connector and writes to the internal
fan-out queue. When it exits, it closes the fan-out queue. That closure is the
terminal signal that lets pooled executors drain remaining messages and stop.

### Output Bridge

The output bridge reads from the internal fan-in queue and writes to the
external output connector. It must remain alive until every executor has
finished. Otherwise, executors could block forever while writing to a full
fan-in queue with no remaining consumer.

### Scaler

Each pooled runner owns a scaler. It:

- Starts the configured initial number of executors.
- Tracks active executors and pending tasks.
- Requests scale-up when queue depth per worker exceeds the configured target.
- Requests scale-down gradually when load falls below the active worker count.
- Stops executor creation when the stage run context is canceled.

Pool configuration controls worker counts, queue sizes, and scaling behavior:

``` go
cfg := processor.NewCustomConfig(goccia.StageRunningModePool)
cfg.Stage.Pool.MinWorkers = 2
cfg.Stage.Pool.MaxWorkers = 8
cfg.Stage.Pool.InputQueueSize = 512
cfg.Stage.Pool.OutputQueueSize = 512
```

## Pooled Shutdown

Pooled shutdown is deliberately ordered. Connector closure, rather than stage
context cancellation alone, drives the drain:

``` text
stage run context is canceled
-> stop accepting scaler start requests
-> input bridge exits and closes fan-out
-> worker executors drain fan-out and finish in-flight work
-> runner waits for every executor
-> runner closes fan-in
-> output bridge drains fan-in and closes external output
-> runner closes the scaler
-> stage Run returns
```

Worker execution and output-bridge reads use a detached context during this
phase. This is intentional: canceling them immediately could strand messages
in an internal queue or leave an executor blocked by downstream backpressure.

The [pipeline lifecycle](pipeline.md#graceful-shutdown) builds on this local
stage behavior by shutting down stages in message-flow order.

## Choosing a Model

Use a custom runner when the stage has source-specific behavior, owns complex
state, or needs lifecycle rules that do not fit independent per-message work.

Use a worker-backed stage in single mode when one executor is enough or
ordering matters.

Use a worker-backed stage in pool mode when messages can be processed
independently and the workload benefits from concurrency.
