---
icon: lucide/table
---

# CSV Decoder Processor

`CSVDecoderStage` converts serializable byte messages into `CSVMessage` rows.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := processor.NewCSVConfig(goccia.StageRunningModeSingle)
cfg.AddColumnDef(processor.NewCSVColumnDef("frame", processor.CSVColumnTypeInt))
cfg.AddColumnDef(processor.NewCSVColumnDef("ts", processor.CSVColumnTypeTimestamp))
stage := processor.NewCSVDecoderStage(in, out, cfg)
```

## Messages

### Input Message

Accepted body requirement: `message.Serializable`.

The decoder reads the byte slice returned by `GetBytes()`.

### Output Message

Produced body type: `*processor.CSVMessage`.

| Field | Description |
| --- | --- |
| `Rows` | Decoded rows, where each row is a list of `*CSVColumn`. |
| `RowCount` | Row-count field carried by the message type. |

Each `CSVColumn` carries `Name`, `Type`, `IsDataValid`, and one type-specific
value field: `StringValue`, `IntValue`, `FloatValue`, `BoolValue`, or
`TimestampValue`.

Additional interfaces: none. `CSVMessage` only implements the standard
`message.Body` contract.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `Columns` | empty | Ordered list of `CSVColumnDef` values. |
| `CSVColumnDef.Name` | constructor arg | Column name in decoded output. |
| `CSVColumnDef.Type` | constructor arg | `String`, `Int`, `Float`, `Bool`, or `Timestamp`. |
| `CSVColumnDef.TimestampLayout` | `time.RFC3339` | Optional layout for timestamp parsing. |

## Internals

The decoder uses Goccia's generic worker-backed processor runner. The parser is
a small in-package comma/newline parser backed by `strings.Builder`,
`strconv`, and `time.Parse`; it does not implement full RFC 4180 quoting or
escaping.
