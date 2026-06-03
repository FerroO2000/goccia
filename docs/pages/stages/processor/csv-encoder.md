---
icon: lucide/file-spreadsheet
---

# CSV Encoder Processor

`CSVEncoderStage` converts `CSVMessage` rows into `CSVEncodedMessage` bytes.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := processor.NewCSVConfig(goccia.StageRunningModeSingle)
cfg.AddColumnDef(processor.NewCSVColumnDef("frame", processor.CSVColumnTypeInt))
stage := processor.NewCSVEncoderStage(in, out, cfg)
```

## Messages

### Input Message

Accepted body type: `*processor.CSVMessage`.

Additional input interfaces: none beyond the standard `message.Body` contract.

### Output Message

Produced body type: `*processor.CSVEncodedMessage`.

| Field | Description |
| --- | --- |
| `Data` | Encoded CSV bytes. |

Additional interfaces: `message.Serializable`. `GetBytes()` returns `Data`.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `Columns` | empty | Ordered list of columns to write. Missing or invalid values use type-specific defaults. |
| `CSVColumnDef.TimestampLayout` | `time.RFC3339` | Layout used when formatting timestamp columns. |

## Internals

The encoder uses Goccia's generic worker-backed processor runner. It builds
rows with `strings.Builder`, formats scalar values with `strconv`, appends a
newline after each row, and emits one encoded byte message per input message.
