---
icon: lucide/gauge
---

# CAN Processor

`CANStage` decodes raw CAN messages into typed signal values.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := processor.NewCANConfig(goccia.StageRunningModePool)
cfg.Messages = []*acmelib.Message{speedMsg, statusMsg}
stage := processor.NewCANStage(in, out, cfg)
```

## Messages

### Input Message

Accepted body requirement: `processor.CANMessageCarrier`.

`CANMessageCarrier` exposes `GetRawMessages() []CANRawMessage`.

### Output Message

Produced body type: `*processor.CANMessage`.

| Field | Description |
| --- | --- |
| `Signals` | Decoded signal values. |
| `SignalCount` | Number of decoded signals. |

Each `CANSignal` carries `CANID`, `Name`, `RawValue`, `Type`, and one
type-specific value field: `ValueFlag`, `ValueInt`, `ValueFloat`, or
`ValueEnum`.

Additional interfaces: none. `CANMessage` only implements the standard
`message.Body` contract.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `Messages` | empty | CAN message definitions from `github.com/squadracorsepolito/acmelib`. |

## Internals

The stage uses Goccia's generic worker-backed processor runner. Decoding is
delegated to `acmelib` signal layouts, keyed by CAN id. Unknown CAN ids produce
no decoded signals.
