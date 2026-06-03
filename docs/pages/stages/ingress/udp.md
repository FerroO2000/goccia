---
icon: lucide/radio-tower
---

# UDP Ingress

`UDPStage` listens on a UDP socket and emits one `UDPMessage` per datagram.

``` go
cfg := ingress.NewUDPConfig()
cfg.Port = 20000
stage := ingress.NewUDPStage(out, cfg)
```

## Messages

### Output Message

Produced body type: `*ingress.UDPMessage`.

| Field | Description |
| --- | --- |
| `Payload` | Backing byte slice for the datagram payload. |
| `PayloadSize` | Number of received payload bytes. |

Additional interfaces: `message.Serializable`. `GetBytes()` returns
`Payload[:PayloadSize]`. Messages are pooled and returned to the pool by
`Destroy()`.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `IPAddr` | `"0.0.0.0"` | Local address to bind. |
| `Port` | `20000` | Local UDP port. |
| `BufferSize` | `1472` | Receive buffer size and default payload capacity. |

## Internals

The stage uses Go's standard `net` package with `net.ListenUDP`. The runner
blocks on `Read`, copies each datagram into a pooled message, records receive
metrics, and writes to the output connector.
