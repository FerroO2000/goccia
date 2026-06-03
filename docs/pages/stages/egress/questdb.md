---
icon: lucide/database
---

# QuestDB Egress

`QuestDBStage` writes rows to QuestDB.

[Pool-capable](../../concepts/stages.md#pooled-execution-mode){ .stage-badge .stage-badge--pool }

``` go
cfg := egress.NewQuestDBConfig(goccia.StageRunningModePool)
cfg.Address = "localhost:9000"
stage := egress.NewQuestDBStage(in, cfg)
```

## Messages

### Input Message

Accepted body type: `*egress.QuestDBMessage`.

Build a `QuestDBMessage` from rows:

``` go
msg := egress.NewQuestDBMessage()
row := egress.NewQuestDBRow("can_signals")
row.AddSymbol(egress.NewQuestDBSymbol("name", "rpm"))
row.AddColumn(egress.NewQuestDBFloatColumn("value", 8123.4))
msg.AddRow(row)
```

Supported column types are bool, int, long, float, string, and timestamp.
Symbols are stored separately because QuestDB symbols must be written before
ordinary columns.

Additional input interfaces: none. `QuestDBMessage` only implements the
standard `message.Body` contract.

This egress stage produces no downstream output message.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `Stage.RunningMode` | constructor arg | `StageRunningModeSingle` or `StageRunningModePool`. |
| `Stage.Pool` | default pool when pool mode is selected | Worker counts, queue sizes, and auto-scaling. |
| `Address` | `"localhost:9000"` | QuestDB endpoint address. |

## Internals

The stage uses `github.com/questdb/go-questdb-client/v3`. Initialization builds
a line-sender pool with HTTP transport, auto-flush rows set to `75000`, and a
one-second retry timeout. Each worker gets a sender from the pool and writes
rows with the message envelope timestamp.
