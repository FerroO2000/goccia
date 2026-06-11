---
icon: lucide/file-input
---

# File Ingress

`FileStage` watches directories and emits file chunks from existing and changed
files.

``` go
cfg := ingress.NewFileConfig()
cfg.WatchedDirs = []string{"./data/in"}
stage := ingress.NewFileStage(out, cfg)
```

## Messages

### Output Message

Produced body type: `*ingress.FileMessage`.

| Field | Description |
| --- | --- |
| `Path` | Path of the file that produced the chunk. |
| `Chunk` | Chunk bytes read from the file. |
| `ChunkSize` | Length of the chunk. |
| `Offset` | Offset of the chunk from the beginning of the file. |
| `DelimiterFound` | Whether `ChunkDelim` was found while growing the chunk. |

Additional interfaces: `message.Serializable`. `GetBytes()` returns `Chunk`.

## Configuration

| Field | Default | Description |
| --- | --- | --- |
| `WatchedDirs` | `[]string{"."}` | Directories watched for file events. |
| `ChunkSize` | `4096` | Initial chunk read size. |
| `CheckChunkDelim` | `true` | Grow a chunk until `ChunkDelim`, EOF, or `MaxChunkSize`. |
| `ChunkDelim` | `'\n'` | Delimiter byte used when delimiter checking is enabled. |
| `MaxChunkSize` | `32 * 1024` | Maximum chunk size when growing to a delimiter. |
| `ForceReRead` | `false` | If true, restart from offset `0` when a reader reopens a file. |
| `CloseDebounce` | `time.Second` | Delay before closing an idle reader after EOF. |

## Internals

The stage uses `github.com/fsnotify/fsnotify` to watch directories. It reads
existing files once at startup, starts one reader per file, uses `bufio` and
`os.File` from the standard library for reads, and fan-ins chunks through an
internal queue before writing to the output connector.
