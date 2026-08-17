---
icon: lucide/braces
---

# JSON Encoder Processor

`JSONEncoderStage` converts typed values held by `JSONMessage` into serialized
JSON bytes.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
type Event struct {
    Name  string `json:"name"`
    Count int    `json:"count"`
}

cfg := processor.NewJSONEncoderConfig(goccia.StageRunningModePool)
stage := processor.NewJSONEncoderStage(in, out, cfg)
```

Create an input body with `processor.NewJSONMessage`:

``` go
body := processor.NewJSONMessage(Event{Name: "ready", Count: 1})
```

## Messages

### Input Message

Accepted body type: `*processor.JSONMessage[T]`.

| Field | Description |
| --- | --- |
| `Data` | Go value passed to `encoding/json`. |

Additional input interfaces: none beyond the standard `message.Body` contract.

### Output Message

Produced body type: `*processor.JSONEncodedMessage`.

| Field | Description |
| --- | --- |
| `Data` | Encoded JSON bytes without a trailing newline. |

Additional interfaces: `message.Serializable`. `GetBytes()` returns `Data`
without copying it; callers must not mutate the slice while the message is in
use.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `Indent` | `""` | Pretty-print indentation inserted for each nesting level. An empty value emits compact JSON. |
| `IndentPrefix` | `""` | Prefix written at the beginning of indented lines. Ignored when `Indent` is empty. |
| `EscapeHTML` | `true` | Escape `<`, `>`, and `&` inside strings as JSON Unicode sequences. |

Use only JSON whitespace in `Indent` and `IndentPrefix`; other characters can
produce output that is not valid JSON. Encoder-specific settings are captured
during stage initialization.

## Metrics

--8<-- "processor/metrics/docs/json_encoder.metrics.doc.md"

### Error Types

--8<-- "processor/metrics/docs/json_encoder.error_types.doc.md"

Successful measurements omit `error.type`; output size is recorded only after
successful encoding.

## Internals

The stage uses Goccia's generic worker-backed processor runner and Go's
`encoding/json` package. Compact output with HTML escaping uses `json.Marshal`;
indented output uses `json.MarshalIndent`. Disabling HTML escaping uses
`json.Encoder`, and the encoder removes its terminating newline before emitting
the message. Each output message preserves the processing span for downstream
stages.
