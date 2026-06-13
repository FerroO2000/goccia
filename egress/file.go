package egress

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/FerroO2000/goccia/connector"
	"github.com/FerroO2000/goccia/egress/metrics"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
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

// ─── Runner ─────────────────────────────────────────────────────────────────|

var _ stage.Runner[*fileEnv] = (*fileRunner[msgSer])(nil)

type fileRunner[T msgSer] struct {
	*fileEnv

	inConnector msgConn[T]

	runDone chan struct{}
}

func newFileRunner[T msgSer](inConnector msgConn[T]) *fileRunner[T] {
	return &fileRunner[T]{
		inConnector: inConnector,

		runDone: make(chan struct{}),
	}
}

func (r *fileRunner[T]) SetEnvironment(env *fileEnv) {
	r.fileEnv = env
}

func (r *fileRunner[T]) Init(_ context.Context) error {
	return nil
}

// Run owns the writer loop, so writes and flushes are never concurrent.
func (r *fileRunner[T]) Run(ctx context.Context) {
	defer close(r.runDone)

	for {
		msgIn, err := r.read(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				if err := r.flush(); err != nil {
					r.Tel.LogError("periodic flush failed", err, "path", r.Config.Path)
				}

				continue
			}

			return
		}

		r.deliver(ctx, msgIn)
		msgIn.Destroy()
	}
}

// read uses a timeout only while data is buffered, turning idle time into a
// flush deadline without a separate ticker goroutine.
func (r *fileRunner[T]) read(ctx context.Context) (*msg[T], error) {
	if r.writer.Buffered() == 0 {
		return r.inConnector.Read(ctx)
	}

	readCtx, cancelReadCtx := context.WithTimeout(ctx, r.Config.FlushDeadline)
	defer cancelReadCtx()

	return r.inConnector.Read(readCtx)
}

func (r *fileRunner[T]) deliver(ctx context.Context, msgIn *msg[T]) {
	if err := r.write(ctx, msgIn); err != nil {
		r.GetEgressMetrics().IncrementDeliveringErrors()
	}

	r.GetEgressMetrics().IncrementDeliveredMessages()
	r.GetEgressMetrics().RecordTotalMessageProcessingTime(
		ctx, int(time.Since(msgIn.GetReceiveTime()).Milliseconds()),
	)
}

func (r *fileRunner[T]) write(ctx context.Context, msgIn *msg[T]) error {
	_, span := r.Tel.StartTrace(msgIn.LoadSpanContext(ctx), "writing file")
	defer span.End()

	chunk := msgIn.GetBody().GetBytes()
	n, err := r.writer.Write(chunk)
	if err != nil {
		r.Tel.LogError("failed to write to file", err, "path", r.Config.Path)
		r.Metrics.IncrementWriteErrors()

		return err
	}

	writtenBytes := int64(n)
	span.SetAttributes(attribute.Int64("chunk_size", writtenBytes))

	if int64(r.writer.Buffered()) >= r.bufSizeThreshold {
		if err := r.flush(); err != nil {
			return err
		}
	}

	r.Metrics.AddWrittenBytes(uint(writtenBytes))

	return nil
}

func (r *fileRunner[T]) flush() error {
	if r.writer.Buffered() == 0 {
		return nil
	}

	if err := r.writer.Flush(); err != nil {
		r.Tel.LogError("failed to flush writer", err, "path", r.Config.Path)
		r.Metrics.IncrementFlushErrors()

		return err
	}

	return nil
}

// Close waits for Run to drain before flushing the remaining buffered bytes.
func (r *fileRunner[T]) Close(_ context.Context) {
	<-r.runDone

	if err := r.flush(); err != nil {
		r.Tel.LogError("failed to flush writer", err, "path", r.Config.Path)
	}
}

func (r *fileRunner[T]) Inputs() []uintptr {
	return []uintptr{connector.GetConnectorID(r.inConnector)}
}

func (r *fileRunner[T]) Outputs() []uintptr {
	return []uintptr{}
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

// FileStage is an egress stage that writes messages to a file sequentially.
// It spawns a single worker that writes messages to the file.
type FileStage[T msgSer] struct {
	*stage.EgressStage[T, *fileEnv]
}

// NewFileStage returns a new file egress stage.
func NewFileStage[T msgSer](inConnector msgConn[T], cfg *FileConfig) *FileStage[T] {
	return &FileStage[T]{
		EgressStage: stage.NewEgressStageFromRunner[T](
			"file", newFileEnv(cfg), newFileRunner(inConnector),
		),
	}
}
