---
icon: lucide/git-merge
---

# Generic Fan-In Ingress

`GenericFanInStage` accepts arbitrary source data, starts one producer loop per
source, and fan-ins generated messages to the pipeline.

``` go
cfg := ingress.NewGenericFanInConfig("accept_clients")
cfg.QueueSize = 1024
stage := ingress.NewGenericFanInStage(handler, out, cfg)
```

## Messages

### Output Message

Produced body type: the handler's `Out` type parameter.

`GenericFanInStage` only requires `Out` to implement `message.Body`. Any
additional interfaces depend on the concrete output body type returned by the
handler.

`HandleData` returns a complete `*message.Message[Out]`, so the handler controls
the message body and any message metadata it sets before writing to the next
stage.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Name` | constructor arg, or `"generic"` when empty | Stage name used in telemetry and the pipeline graph. |
| `QueueSize` | `512` | Internal fan-in queue size between per-source producer loops and the output connector. |

## Handler

The handler implements `Init(ctx)`, `HandleSource(ctx)`,
`OnRunContextDone()`, `HandleData(ctx, data)`,
`OnHandleDataContextDone()`, `Close()`, and `SetTelemetry(tel)`. Embed
`ingress.GenericFanInHandlerBase` when you only need to implement source and
data handling.

`HandleSource` is called in the main loop and returns arbitrary `Data` for a
new producer goroutine. For example, a TCP-like source can return an accepted
connection.

`HandleData` is called repeatedly in the per-source goroutine and returns
messages to emit. Return `ingress.ErrQuitLoop` from `HandleSource` to stop the
main loop, or from `HandleData` to stop only that source loop. Other errors are
logged and the relevant loop continues.

## Internals

The stage uses a custom fan-in ingress runner. The main loop discovers source
data, each source goroutine writes generated messages to an internal fan-in
queue, and an output bridge drains that queue into the stage output connector.
