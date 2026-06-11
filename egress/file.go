package egress

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/egress/metrics"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"go.opentelemetry.io/otel/attribute"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

// Default values for the file egress stage configuration.
const (
	DefaultFileConfigBufferSize               = 4096
	DefaultFileConfigFlushThresholdPercentage = 0.75
	DefaultFileConfigFlushDeadline            = time.Second
)

// FileConfig structs contains the configuration for the file egress stage.
type FileConfig struct {
	// Path is the path to the file.
	Path string

	// BufferSize is the size of the buffer used to write messages to the file.
	BufferSize int

	// FlushThresholdPercentage is the percentage of the buffer size that triggers a flush.
	FlushThresholdPercentage float64

	// FlushDeadline is the maximum time to wait before flushing the buffer.
	FlushDeadline time.Duration
}

// NewFileConfig returns the default configuration for the file egress stage.
func NewFileConfig(path string) *FileConfig {
	return &FileConfig{
		Path:                     path,
		BufferSize:               DefaultFileConfigBufferSize,
		FlushThresholdPercentage: DefaultFileConfigFlushThresholdPercentage,
		FlushDeadline:            DefaultFileConfigFlushDeadline,
	}
}

// Validate checks the configuration.
func (c *FileConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckNotNegative(ac, "BufferSize", &c.BufferSize, DefaultFileConfigBufferSize)
	config.CheckNotZero(ac, "BufferSize", &c.BufferSize, DefaultFileConfigBufferSize)

	config.CheckNotNegative(ac, "FlushThresholdPercentage", &c.FlushThresholdPercentage, DefaultFileConfigFlushThresholdPercentage)
	config.CheckNotZero(ac, "FlushThresholdPercentage", &c.FlushThresholdPercentage, DefaultFileConfigFlushThresholdPercentage)
	config.CheckNotGreaterThan(ac, "FlushThresholdPercentage", "1.0", &c.FlushThresholdPercentage, 1.0)

	config.CheckNotNegative(ac, "FlushDeadline", &c.FlushDeadline, DefaultFileConfigFlushDeadline)
	config.CheckNotZero(ac, "FlushDeadline", &c.FlushDeadline, DefaultFileConfigFlushDeadline)
}

// ─── Environment ────────────────────────────────────────────────────────────|

type fileEnv struct {
	*env.BaseEnv[*FileConfig, *metrics.FileStage]

	file             *os.File
	writer           *bufio.Writer
	bufSizeThreshold int64
}

func newFileEnv(config *FileConfig) *fileEnv {
	bufSizeThreshold := int64(float64(config.BufferSize) * config.FlushThresholdPercentage)

	return &fileEnv{
		BaseEnv: env.NewEgressEnv(config, metrics.NewFileStage()),

		writer:           nil,
		bufSizeThreshold: bufSizeThreshold,
	}
}

func (fe *fileEnv) Init(ctx context.Context) error {
	path := fe.Config.Path

	// Get the directory path from the file path
	dir := filepath.Dir(path)

	// Create all parent directories if they don't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Open the file as append only
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	fe.file = file

	// Create the bufio writer
	fe.writer = bufio.NewWriterSize(file, fe.Config.BufferSize)

	return fe.BaseEnv.Init(ctx)
}

func (fe *fileEnv) Close(ctx context.Context) {
	// Sync and close the file
	if err := fe.file.Sync(); err != nil {
		fe.Tel.LogError("failed to sync file", err, "path", fe.Config.Path)
	}

	if err := fe.file.Close(); err != nil {
		fe.Tel.LogError("failed to close file", err, "path", fe.Config.Path)
	}

	fe.BaseEnv.Close(ctx)
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type fileWorker[T msgSer] struct {
	worker.BaseWorker[*fileEnv]

	ticker   *time.Ticker
	tickerWg *sync.WaitGroup
	flushMux *sync.Mutex

	notFlushedBytes atomic.Int64
}

func newFileWorkerMaker[T msgSer]() func() *fileWorker[T] {
	return func() *fileWorker[T] {
		return &fileWorker[T]{
			tickerWg: &sync.WaitGroup{},
			flushMux: &sync.Mutex{},
		}
	}
}

func (fw *fileWorker[T]) Init(ctx context.Context) error {
	// Create the ticker
	fw.ticker = time.NewTicker(fw.Env.Config.FlushDeadline)
	go fw.runTicker(ctx)

	return fw.BaseWorker.Init(ctx)
}

func (fw *fileWorker[T]) runTicker(ctx context.Context) {
	fw.tickerWg.Add(1)
	defer fw.tickerWg.Done()

	defer fw.ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-fw.ticker.C:
			if err := fw.flush(); err != nil {
				fw.Tel.LogError("periodic flush failed", err, "path", fw.Env.Config.Path)
			}
		}
	}
}

func (fw *fileWorker[T]) Deliver(ctx context.Context, msgIn *msg[T]) error {
	ctx, span := fw.Tel.StartTrace(ctx, "writing file")
	defer span.End()

	// Write message bytes to file
	chunk := msgIn.GetBody().GetBytes()
	n, err := fw.Env.writer.Write(chunk)
	if err != nil {
		fw.Tel.LogError("failed to write to file", err, "path", fw.Env.Config.Path)
		fw.Env.Metrics.IncrementWriteErrors()

		return err
	}

	writtenBytes := int64(n)
	bytesUnflushed := fw.notFlushedBytes.Add(writtenBytes)

	span.SetAttributes(attribute.Int64("chunk_size", writtenBytes))

	// Check wether to flush the writer
	if bytesUnflushed >= fw.Env.bufSizeThreshold {
		if err := fw.flush(); err != nil {
			return err
		}
	}

	// Update metrics
	fw.Env.Metrics.AddWrittenBytes(uint(writtenBytes))

	return nil
}

func (fw *fileWorker[T]) flush() error {
	fw.flushMux.Lock()
	defer fw.flushMux.Unlock()

	// Check if there is anything to flush
	if fw.notFlushedBytes.Load() == 0 {
		return nil
	}

	if err := fw.Env.writer.Flush(); err != nil {
		fw.Tel.LogError("failed to flush writer", err, "path", fw.Env.Config.Path)
		fw.Env.Metrics.IncrementFlushErrors()

		return err
	}

	fw.notFlushedBytes.Store(0)

	return nil
}

func (fw *fileWorker[T]) Close(ctx context.Context) error {
	fw.tickerWg.Wait()
	if err := fw.flush(); err != nil {
		return err
	}

	return fw.BaseWorker.Close(ctx)
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// FileStage is an egress stage that writes messages to a file sequentially.
// It spawns a single worker that writes messages to the file.
type FileStage[T msgSer] struct {
	*stage.EgressStage[T, *fileEnv]
}

// NewFileStage returns a new file egress stage.
func NewFileStage[T msgSer](inConnector msgConn[T], cfg *FileConfig) *FileStage[T] {
	env := newFileEnv(cfg)

	return &FileStage[T]{
		EgressStage: stage.NewEgressStage(
			"file", inConnector, env, newFileWorkerMaker[T](), &config.Stage{
				RunningMode: config.StageRunningModeSingle,
			},
		),
	}
}
