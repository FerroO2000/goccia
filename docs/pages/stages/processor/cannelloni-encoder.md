---
icon: lucide/package
---

# Cannelloni Encoder Processor

`CannelloniEncoderStage` encodes `CannelloniMessage` values into Cannelloni
protocol bytes.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := processor.NewCannelloniConfig(goccia.StageRunningModeSingle)
stage := processor.NewCannelloniEncoderStage(in, out, cfg)
```

## Messages

### Input Message

Accepted body type: `*processor.CannelloniMessage`.

Additional input interfaces: `message.ReOrderable` and
`processor.CANMessageCarrier`, as implemented by `CannelloniMessage`.

### Output Message

Produced body type: `*processor.CannelloniEncodedMessage`.

| Field or method | Description |
| --- | --- |
| `GetBytes()` | Encoded Cannelloni frame bytes. |

Additional interfaces: `message.Serializable`. The encoded payload field is
private; use `GetBytes()` to pass it to byte-oriented egress stages.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |

## Internals

The encoder uses the generic worker-backed processor runner. It writes
Cannelloni frame headers and raw CAN messages manually with big-endian binary
encoding, using version `1` and opcode `0`.
