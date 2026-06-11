---
icon: lucide/timer
---

# Ticker Ingress

`TickerStage` emits a `TickerMessage` at a fixed interval.

``` go
cfg := ingress.NewTickerConfig()
stage := ingress.NewTickerStage(out, cfg)
```

## Messages

### Output Message

Produced body type: `*ingress.TickerMessage`.

| Field | Description |
| --- | --- |
| `TickNumber` | Tick counter, starting at `1` and incrementing while the stage runs. |

Additional interfaces: none. `TickerMessage` only implements the standard
`message.Body` contract.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Interval` | `100 * time.Millisecond` | Time between generated ticks. Must be positive. |

## Internals

The runner uses Go's standard `time.Ticker`. Each tick creates a new message,
sets receive time and timestamp to `time.Now()`, records telemetry, and writes
to the output connector.
