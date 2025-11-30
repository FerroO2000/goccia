package egress

import (
	"bufio"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/pool"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

// FileConfig structs contains the configuration for the file egress stage.
type FileConfig struct {
	// Path is the path to the file.
	Path string

	// BufferSize is the size of the buffer used to write messages to the file.
	//
	// Default: 4096
	BufferSize int

	// FlushThresholdPercentage is the percentage of the buffer size that triggers a flush.
	//
	// Default: 0.75
	FlushThresholdPercentage float64

	// FlushDeadline is the maximum time to wait before flushing the buffer.
	//
	// Default: 1s
	FlushDeadline time.Duration
}

// DefaultFileConfig returns the default configuration for the file egress stage.
func DefaultFileConfig(path string) *FileConfig {
	return &FileConfig{
		Path:                     path,
		BufferSize:               4096,
		FlushThresholdPercentage: 0.75,
		FlushDeadline:            time.Second,
	}
}

////////////////////////
//  WORKER ARGUMENTS  //
////////////////////////

type fileWorkerArgs struct {
	writer           *bufio.Writer
	path             string
	bufSizeThreshold int64
	flushDeadline    time.Duration
}

func newFileWorkerArgs(
	writer *bufio.Writer, path string, bufSizeThreshold int64, flushDeadline time.Duration,
) *fileWorkerArgs {

	return &fileWorkerArgs{
		writer:           writer,
		path:             path,
		bufSizeThreshold: bufSizeThreshold,
		flushDeadline:    flushDeadline,
	}
}

//////////////////////
//  WORKER METRICS  //
//////////////////////

type fileWorkerMetrics struct {
	once sync.Once

	writtenBytes atomic.Int64
	writeErrors  atomic.Int64
	flushErrors  atomic.Int64
}

var fileWorkerMetricsInst = &fileWorkerMetrics{}

func (fwm *fileWorkerMetrics) init(tel *internal.Telemetry) {
	fwm.once.Do(func() {
		fwm.initMetrics(tel)
	})
}

func (fwm *fileWorkerMetrics) initMetrics(tel *internal.Telemetry) {
	tel.NewCounter("written_bytes", func() int64 { return fwm.writtenBytes.Load() })
	tel.NewCounter("write_errors", func() int64 { return fwm.writeErrors.Load() })
	tel.NewCounter("flush_errors", func() int64 { return fwm.flushErrors.Load() })
}

func (fwm *fileWorkerMetrics) addWrittenBytes(amount int64) {
	fwm.writtenBytes.Add(amount)
}

func (fwm *fileWorkerMetrics) incrementWriteErrors() {
	fwm.writeErrors.Add(1)
}

func (fwm *fileWorkerMetrics) incrementFlushErrors() {
	fwm.flushErrors.Add(1)
}

/////////////////////////////
//  WORKER IMPLEMENTATION  //
/////////////////////////////

type fileWorker[T msgSer] struct {
	pool.BaseWorker

	writer *bufio.Writer

	ticker   *time.Ticker
	tickerWg *sync.WaitGroup
	flushMux *sync.Mutex

	path             string
	bufSizeThreshold int64

	notFlushedBytes atomic.Int64

	metrics *fileWorkerMetrics
}

func newFileWorkerInstMaker[T msgSer]() workerInstanceMaker[*fileWorkerArgs, T] {
	return func() workerInstance[*fileWorkerArgs, T] {
		return &fileWorker[T]{
			tickerWg: &sync.WaitGroup{},
			flushMux: &sync.Mutex{},

			metrics: fileWorkerMetricsInst,
		}
	}
}

func (fw *fileWorker[T]) Init(ctx context.Context, args *fileWorkerArgs) error {
	fw.writer = args.writer

	fw.path = args.path

	// Set the thresholds for flushing the buffer
	fw.bufSizeThreshold = args.bufSizeThreshold

	fw.metrics.init(fw.Tel)

	// Create the ticker
	fw.ticker = time.NewTicker(args.flushDeadline)
	go fw.runTicker(ctx)

	return nil
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
				fw.Tel.LogError("periodic flush failed", err, "path", fw.path)
			}
		}
	}
}

func (fw *fileWorker[T]) Deliver(ctx context.Context, msgIn *msg[T]) error {
	ctx, span := fw.Tel.NewTrace(ctx, "writing file")
	defer span.End()

	// Write message bytes to file
	chunk := msgIn.GetEnvelope().GetBytes()
	n, err := fw.writer.Write(chunk)
	if err != nil {
		fw.Tel.LogError("failed to write to file", err, "path", fw.path)
		fw.metrics.incrementWriteErrors()

		return err
	}

	writtenBytes := int64(n)
	bytesUnflushed := fw.notFlushedBytes.Add(writtenBytes)

	span.SetAttributes(attribute.Int64("chunk_size", writtenBytes))

	// Check wether to flush the writer
	if bytesUnflushed >= fw.bufSizeThreshold {
		if err := fw.flush(); err != nil {
			return err
		}
	}

	// Update metrics
	fw.metrics.addWrittenBytes(writtenBytes)

	return nil
}

func (fw *fileWorker[T]) flush() error {
	fw.flushMux.Lock()
	defer fw.flushMux.Unlock()

	// Check if there is anything to flush
	if fw.notFlushedBytes.Load() == 0 {
		return nil
	}

	if err := fw.writer.Flush(); err != nil {
		fw.Tel.LogError("failed to flush writer", err, "path", fw.path)
		fw.metrics.incrementFlushErrors()

		return err
	}

	fw.notFlushedBytes.Store(0)

	return nil
}

func (fw *fileWorker[T]) Close(_ context.Context) error {
	fw.tickerWg.Wait()
	return fw.flush()
}

/////////////
//  STAGE  //
/////////////

// FileStage is an egress stage that writes messages to a file sequentially.
// It spawns a single worker that writes messages to the file.
type FileStage[T msgSer] struct {
	stage[*fileWorkerArgs, T]

	cfg *FileConfig

	file *os.File
}

// NewFileStage returns a new file egress stage.
func NewFileStage[T msgSer](inputConnector msgConn[T], cfg *FileConfig) *FileStage[T] {
	return &FileStage[T]{
		stage: newStageSingle("file", inputConnector, newFileWorkerInstMaker[T]()),

		cfg: cfg,
	}
}

// Init initializes the stage.
func (fs *FileStage[T]) Init(ctx context.Context) error {
	path := fs.cfg.Path

	// Open the file as append only
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	fs.file = file

	// Create the bufio writer
	writer := bufio.NewWriterSize(file, fs.cfg.BufferSize)

	// Create the worker arguments
	bufSizeThreshold := int64(float64(fs.cfg.BufferSize) * fs.cfg.FlushThresholdPercentage)
	workerArgs := newFileWorkerArgs(writer, path, bufSizeThreshold, fs.cfg.FlushDeadline)

	return fs.stage.Init(ctx, workerArgs)
}

// Close closes the stage.
func (fs *FileStage[T]) Close() {
	fs.stage.Close()

	// Sync and close the file
	if err := fs.file.Sync(); err != nil {
		fs.Tel().LogError("failed to sync file", err, "path", fs.cfg.Path)
	}

	if err := fs.file.Close(); err != nil {
		fs.Tel().LogError("failed to close file", err, "path", fs.cfg.Path)
	}
}
