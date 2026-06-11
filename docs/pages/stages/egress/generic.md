---
icon: lucide/code
---

# Generic Egress

`GenericStage` runs a user-provided handler for each input message and
terminates the pipeline branch.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := egress.NewGenericConfig("publish_metrics", goccia.StageRunningModePool)
stage := egress.NewGenericStage(handler, in, cfg)
```

## Messages

### Input Message

Accepted body type: the handler's `In` type parameter.

`GenericStage` only requires `In` to implement `message.Body`. Any additional
interfaces depend on the concrete input body type and on the logic implemented
by the handler.

This egress stage produces no downstream output message.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `Name` | constructor arg, or `"generic"` when empty | Stage name used in telemetry and the pipeline graph. |

## Handler

The handler implements `Init(ctx)`, `Handle(ctx, msgIn)`, `Close()`, and
`SetTelemetry(tel)`. Embed `egress.GenericHandlerBase` when you only need to
implement `Handle`.

`Handle` receives the message body and returns an error. Returning an error
marks the delivery as failed for that message.

## Internals

The stage uses Goccia's generic worker-backed egress runner. Each worker
restores the message context, starts a handler span, and calls the handler
without writing to an output connector.
