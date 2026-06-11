---
icon: lucide/code
---

# Generic Processor

`GenericStage` runs a user-provided handler for each input message.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := processor.NewGenericConfig(goccia.StageRunningModePool)
cfg.Name = "decode_payload"
stage := processor.NewGenericStage(handler, in, out, cfg)
```

## Messages

### Input Message

Accepted body type: the handler's `In` type parameter.

`GenericStage` only requires `In` to implement `message.Body`. Any additional
interfaces depend on the concrete input body type.

### Output Message

Produced body type: the handler's `Out` type parameter.

`GenericStage` only requires the returned body to implement `message.Body`.
Additional interfaces are not added by the stage; if `Out` also implements
`message.Serializable`, `message.ReOrderable`, or an application-specific
interface, that behavior comes from the concrete type returned by the handler.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `Name` | `"generic"` | Stage name used in telemetry and the pipeline graph. |

## Handler

The handler implements `Init(ctx)`, `Handle(ctx, msgIn)`, `Close()`, and
`SetTelemetry(tel)`. Embed `processor.GenericHandlerBase` when you only need to
implement `Handle`.

## Internals

The stage uses Goccia's generic worker-backed processor runner. Each worker
calls the handler, wraps the returned body in a new message envelope, and saves
the handler span for downstream stages.
