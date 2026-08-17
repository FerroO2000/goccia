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
| `Path` | constructor arg | Output file path. When rotation is enabled, it is interpreted as a Go time layout. Parent directories are created automatically. |
| `BufferSize` | `4096` | `bufio.Writer` buffer size. |
| `FlushThresholdPercentage` | `0.75` | Flush when buffered bytes reach this fraction of `BufferSize`. |
| `FlushDeadline` | `time.Second` | Maximum idle time before flushing buffered bytes. |
| `RotationEnable` | `false` | Enables output file rotation. |
| `RotationInterval` | `time.Hour` | Output file rotation interval. Used only when rotation is enabled. |

File egress always runs with a custom single runner and does not expose the
generic worker-pool config.

When rotation is enabled, the stage checks the interval before each write. If
the interval elapsed, it flushes, syncs, and closes the current file as it
switches to the next path. For example:

``` go
cfg := egress.NewFileConfig("./data/out/out-20060102.txt")
cfg.RotationEnable = true
cfg.RotationInterval = 24 * time.Hour
```

When rotation is enabled, choose a `Path` layout that changes at the configured
interval. For example, hourly rotation should include the hour in the path.

## Metrics

--8<-- "egress/metrics/docs/file_stage.metrics.doc.md"

## Internals

The stage uses `os.File` and `bufio.Writer` from the standard library. It opens
the file append-only and writes each message byte slice exactly as returned by
`GetBytes()`.

A single runner goroutine owns the writer. It flushes when the buffer reaches
the configured threshold, after `FlushDeadline` of input idleness while data is
buffered, before rotation, and during stage close before the file is synced and
closed.
