---
icon: lucide/refresh-cw
---

# Generic Loop Ingress

`GenericLoopStage` runs a user-provided loop that creates messages for the
pipeline.

``` go
cfg := ingress.NewGenericConfig("read_sensor")
stage := ingress.NewGenericLoopStage(handler, out, cfg)
```

## Messages

### Output Message

Produced body type: the handler's `Out` type parameter.

`GenericLoopStage` only requires `Out` to implement `message.Body`. Any
additional interfaces depend on the concrete output body type returned by the
handler.

The handler returns a complete `*message.Message[Out]`, so it controls the
message body and any message metadata it sets before writing to the next stage.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Name` | constructor arg, or `"generic"` when empty | Stage name used in telemetry and the pipeline graph. |

## Handler

The handler implements `Init(ctx)`, `Handle(ctx)`, `OnRunContextDone()`,
`Close()`, and `SetTelemetry(tel)`. Embed `ingress.GenericLoopHandlerBase` when
you only need to implement `Handle`.

`Handle` is called repeatedly until the stage context is canceled or it returns
`ingress.ErrQuitLoop`. Other errors are logged and the loop continues.

`OnRunContextDone` is called when the running context is canceled, so blocking
source logic can be interrupted or cleaned up.

## Internals

The stage uses a custom single ingress runner. Each successful handler call
writes the returned message to the output connector. If the output write fails,
the message is destroyed and the runner continues.
