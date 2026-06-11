---
icon: lucide/network
---

# TCP Egress

`TCPStage` writes serializable message bytes to one TCP connection.

``` go
cfg := egress.NewTCPConfig()
cfg.IPAddr = "127.0.0.1"
cfg.Port = 20000
stage := egress.NewTCPStage(in, cfg)
```

## Messages

### Input Message

Accepted body requirement: `message.Serializable`.

The stage writes the byte slice returned by `GetBytes()` to the TCP connection.

This egress stage produces no downstream output message.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `IPAddr` | `"127.0.0.1"` | Destination address. |
| `Port` | `20000` | Destination TCP port. |
| `WriteTimeout` | `10 * time.Second` | Deadline applied before each write. |

TCP egress always runs in single mode and does not expose the generic
worker-pool config.

## Internals

The stage uses Go's standard `net` package. Initialization dials a
`net.TCPConn`; the worker sets a write deadline, writes the full byte slice
returned by `GetBytes()`, and closes the connection when the stage closes.
