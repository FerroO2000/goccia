---
icon: lucide/file-output
---

# File Egress

`FileStage` appends serializable message bytes to a file.

``` go
cfg := egress.NewFileConfig("./data/out/out.txt")
stage := egress.NewFileStage(in, cfg)
```

## Messages

### Input Message

Accepted body requirement: `message.Serializable`.

The stage appends the byte slice returned by `GetBytes()` to the configured
file.

This egress stage produces no downstream output message.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Path` | constructor arg | Output file path. Parent directories are created automatically. |
| `BufferSize` | `4096` | `bufio.Writer` buffer size. |
| `FlushThresholdPercentage` | `0.75` | Flush when pending bytes reach this fraction of `BufferSize`. |
| `FlushDeadline` | `time.Second` | Periodic flush interval. |

File egress always runs in single mode and does not expose the generic
worker-pool config.

## Internals

The stage uses `os.File` and `bufio.Writer` from the standard library. It opens
the file append-only, writes each message byte slice, flushes on threshold or
deadline, and syncs/closes the file during stage close.
