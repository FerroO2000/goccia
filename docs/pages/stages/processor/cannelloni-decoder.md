---
icon: lucide/binary
---

# Cannelloni Decoder Processor

`CannelloniDecoderStage` decodes Cannelloni protocol bytes into
`CannelloniMessage` values carrying raw CAN messages.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := processor.NewCannelloniConfig(goccia.StageRunningModePool)
stage := processor.NewCannelloniDecoderStage(in, out, cfg)
```

## Messages

### Input Message

Accepted body requirement: `message.Serializable`.

The decoder reads the Cannelloni frame bytes returned by `GetBytes()`.

### Output Message

Produced body type: `*processor.CannelloniMessage`.

| Field or method | Description |
| --- | --- |
| `Messages` | Raw CAN messages decoded from the Cannelloni frame. |
| `MessageCount` | Number of raw CAN messages. |
| `GetSequenceNumber()` | Frame sequence number exposed for reordering. |
| `GetRawMessages()` | Raw CAN messages exposed for `CANStage`. |

Additional interfaces: `message.ReOrderable` and
`processor.CANMessageCarrier`. This means decoder output can pass through
`ROBStage` and then into `CANStage`.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |

## Internals

The decoder uses the generic worker-backed processor runner. It parses the
Cannelloni frame manually with big-endian binary reads, validates basic frame
lengths, and converts frame messages into raw CAN messages.
