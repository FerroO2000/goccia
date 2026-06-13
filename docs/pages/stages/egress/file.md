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
| `FlushThresholdPercentage` | `0.75` | Flush when buffered bytes reach this fraction of `BufferSize`. |
| `FlushDeadline` | `time.Second` | Maximum idle time before flushing buffered bytes. |

File egress always runs with a custom single runner and does not expose the
generic worker-pool config.

## Internals

The stage uses `os.File` and `bufio.Writer` from the standard library. It opens
the file append-only and writes each message byte slice exactly as returned by
`GetBytes()`.

A single runner goroutine owns the writer. It flushes when the buffer reaches
the configured threshold, after `FlushDeadline` of input idleness while data is
buffered, and during stage close before the file is synced and closed.
