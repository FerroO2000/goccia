---
icon: lucide/braces
---

# JSON Decoder Processor

`JSONDecoderStage` converts serializable JSON bytes into typed Go values held
by `JSONMessage`.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
type Event struct {
    Name  string `json:"name"`
    Count int    `json:"count"`
}

cfg := processor.NewJSONDecoderConfig(goccia.StageRunningModePool)
cfg.DisallowUnknownFields = true
stage := processor.NewJSONDecoderStage[InputMessage, *Event](in, out, cfg)
```

## Messages

### Input Message

Accepted body requirement: `message.Serializable`.

The decoder reads the byte slice returned by `GetBytes()`. Leading and trailing
JSON whitespace is accepted and counts toward `MaxInputBytes`.

### Output Message

Produced body type: `*processor.JSONMessage[Out]`.

| Field | Description |
| --- | --- |
| `Data` | Value decoded by Go's `encoding/json` package. |

Additional interfaces: none. `JSONMessage` only implements the standard
`message.Body` contract.

`Out` can be any type supported by `encoding/json`. A pointer type is useful
when the application needs to distinguish a decoded value from `null`: unless
`RejectNull` is enabled, a top-level `null` produces a nil pointer.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `DisallowUnknownFields` | `false` | Reject unknown object keys when decoding into a struct. Maps and interface values are unaffected. |
| `UseNumber` | `false` | Decode numbers stored in interface values as `json.Number` instead of `float64`. Typed numeric fields are unaffected. |
| `MaxInputBytes` | `0` | Maximum input size, including surrounding whitespace. Zero disables the limit; negative values are reset to zero during validation. |
| `RejectNull` | `false` | Reject a top-level JSON `null`. Nested null values remain allowed. |

Decoder-specific settings are captured during stage initialization. Update the
configuration before calling `Pipeline.Init`.

The decoder accepts exactly one top-level JSON value. Empty, malformed,
truncated, type-incompatible, oversized, and disallowed inputs return an error
to the processor runner. The runner records the processing failure and applies
its configured error behavior.

## Metrics

--8<-- "processor/metrics/docs/json_decoder.metrics.doc.md"

### Error Types

--8<-- "processor/metrics/docs/json_decoder.error_types.doc.md"

Successful measurements omit `error.type`.

## Internals

The stage uses Goccia's generic worker-backed processor runner and Go's
`encoding/json` package. The default path uses `json.Unmarshal`; enabling
`DisallowUnknownFields` or `UseNumber` switches to `json.Decoder`. Each output
message preserves the processing span for downstream stages.

Continue to the [JSON encoder processor](json-encoder.md).
