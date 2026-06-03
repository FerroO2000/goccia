---
icon: lucide/network
---

# TCP Ingress

`TCPStage` listens for TCP connections and emits one `TCPMessage` per framed
message.

``` go
cfg := ingress.NewTCPConfig()
cfg.Port = 20000
stage := ingress.NewTCPStage(out, &cfg)
```

## Messages

### Output Message

Produced body type: `*ingress.TCPMessage`.

| Field | Description |
| --- | --- |
| `RemoteAddr` | Remote peer address for the accepted connection. |
| `Message` | Framed payload bytes. |
| `MessageSize` | Payload length in bytes. |

Additional interfaces: `message.Serializable`. `GetBytes()` returns `Message`,
so TCP ingress output can feed serializable processors and byte-oriented egress
stages.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `IPAddr` | `"0.0.0.0"` | Local address to bind. |
| `Port` | `20000` | Local TCP port. |
| `BufferSize` | `4096` constant | Read buffer size. Set this explicitly when using `NewTCPConfig()`, because the constructor currently leaves it at Go's zero value. |
| `ReadTimeout` | `10 * time.Second` | Per-read timeout for accepted connections. |
| `FramingMode` | `TCPFramingModeDelimited` | Message framing strategy. |
| `MaxMessageSize` | `4 << 20` | Maximum accumulated frame size before closing a connection. |
| `Delimiter` | `"\r\n"` | Delimiter used by delimited framing. |
| `HeaderLen` | `16` constant | Header size for length-prefixed framing. |
| `MessageLengthFieldLen` | none | Length-field width for length-prefixed framing. |
| `MessageLengthFieldOffset` | `0` | Offset of the length field inside the header. |
| `MessageLengthFieldEndianess` | `LittleEndian` zero value | Byte order for the length field. |
| `OutputQueueSize` | `512` | Fan-in queue between connection goroutines and the output connector. |

## Framing

Use `TCPFramingModeDelimited` for delimiter-separated messages. Use
`TCPFramingModeLengthPrefixed` when a fixed header contains the payload length.
For length-prefixed mode, set `HeaderLen`, `MessageLengthFieldLen`,
`MessageLengthFieldOffset`, and `MessageLengthFieldEndianess`.

## Internals

The stage uses Go's standard `net` package. It owns a `net.TCPListener`, starts
one goroutine per accepted connection, uses a buffer `sync.Pool`, and fan-ins
decoded messages through an internal queue before writing to the output
connector.
