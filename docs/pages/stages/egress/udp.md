---
icon: lucide/radio-tower
---

# UDP Egress

`UDPStage` sends each serializable message as one UDP datagram.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := egress.NewUDPConfig(goccia.StageRunningModePool)
cfg.IPAddr = "127.0.0.1"
cfg.Port = 20000
stage := egress.NewUDPStage(in, cfg)
```

## Messages

### Input Message

Accepted body requirement: `message.Serializable`.

The stage sends the byte slice returned by `GetBytes()` as one UDP datagram.

This egress stage produces no downstream output message.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `IPAddr` | `"127.0.0.1"` | Destination address. |
| `Port` | `20000` | Destination UDP port. |

## Internals

The stage uses Go's standard `net` package. Initialization creates a connected
`net.UDPConn` with `net.DialUDP`; each worker writes the byte slice returned by
`GetBytes()` and records delivered-byte metrics.
