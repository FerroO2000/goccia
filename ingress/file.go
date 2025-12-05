package ingress

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FerroO2000/goccia/internal"
	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/pool"
	"github.com/fsnotify/fsnotify"
	"go.opentelemetry.io/otel/attribute"
)

//////////////
//  CONFIG  //
//////////////

// Default values for the file ingress stage configuration.
const (
	DefaultFileConfigChunkSize       = 4096
	DefaultFileConfigCheckChunkDelim = true
	DefaultFileConfigChunkDelim      = '\n'
	DefaultFileConfigMaxChunkSize    = 32 * 1024
	DefaultFileConfigForceReRead     = false
	DefaultFileConfigCloseDebounce   = time.Second
)

// DefaultFileConfigWatchedDirs is the default list of directories to watch.
var DefaultFileConfigWatchedDirs = []string{"."}

// FileConfig structs contains the configuration for the file ingress stage.
type FileConfig struct {
	// WatchedDirs contains the list of directories to watch.
	WatchedDirs []string

	// ChunkSize is the size of the chunks to read from a file.
	ChunkSize int

	// CheckChunkDelim states wether to check for a delimiter byte.
	// If true, the reader will grow the chunk until the delimiter (or EOF) is found.
	// This option allows the reader to behave like an hybrid between a chunked reader
	// and a line reader.
	CheckChunkDelim bool

	// ChunkDelim is the delimiter byte used to grow the chunks.
	// It is only used if CheckChunkDelim is true.
	ChunkDelim byte

	// MaxChunkSize is the maximum size of the chunks to read from a file.
	// It is only used if CheckChunkDelim is true.
	MaxChunkSize int

	// ForceReRead states wether to re-read the files after the reader is closed.
	// If false, the reader will seek to the last read offset.
	ForceReRead bool

	// CloseDebounce is the duration to wait before closing the reader (file).
	// This is useful to avoid closing and re-opening the file too often
	// in scenarios where the file is being frequently modified.
	CloseDebounce time.Duration
}

// NewFileConfig returns the default configuration for the file ingress stage.
func NewFileConfig() *FileConfig {
	return &FileConfig{
		WatchedDirs:     DefaultFileConfigWatchedDirs,
		ChunkSize:       DefaultFileConfigChunkSize,
		CheckChunkDelim: DefaultFileConfigCheckChunkDelim,
		ChunkDelim:      DefaultFileConfigChunkDelim,
		MaxChunkSize:    DefaultFileConfigMaxChunkSize,
		ForceReRead:     DefaultFileConfigForceReRead,
		CloseDebounce:   DefaultFileConfigCloseDebounce,
	}
}

// Validate checks the configuration.
func (c *FileConfig) Validate(ac *config.AnomalyCollector) {
	config.CheckLen(ac, "WatchedDirs", &c.WatchedDirs, DefaultFileConfigWatchedDirs)

	config.CheckNotNegative(ac, "ChunkSize", &c.ChunkSize, DefaultFileConfigChunkSize)
	config.CheckNotZero(ac, "ChunkSize", &c.ChunkSize, DefaultFileConfigChunkSize)

	config.CheckNotNegative(ac, "MaxChunkSize", &c.MaxChunkSize, DefaultFileConfigMaxChunkSize)
	config.CheckNotZero(ac, "MaxChunkSize", &c.MaxChunkSize, DefaultFileConfigMaxChunkSize)

	config.CheckNotNegative(ac, "CloseDebounce", &c.CloseDebounce, DefaultFileConfigCloseDebounce)
}

func (c *FileConfig) toReaderConfig(filePath string) *fileReaderConfig {
	return &fileReaderConfig{
		filePath:        filePath,
		chunkSize:       c.ChunkSize,
		checkChunkDelim: c.CheckChunkDelim,
		chunkDelim:      c.ChunkDelim,
		maxChunkSize:    c.MaxChunkSize,
		closeDebounce:   c.CloseDebounce,
		forceReRead:     c.ForceReRead,
	}
}

///////////////
//  MESSAGE  //
///////////////

var _ msgSer = (*FileMessage)(nil)

// FileMessage represents a message returned by the file ingress stage.
type FileMessage struct {
	// Path is the path of the file.
	Path string

	// Chunk is the file contents.
	Chunk []byte

	// ChunkSize is the length of the chunk (content).
	ChunkSize int

	// Offset is the offset of the chunk from the beginning of the file.
	Offset int64

	// DelimiterFound states wether the delimiter byte was found in the chunk.
	DelimiterFound bool
}

func newFileMessage() *FileMessage {
	return &FileMessage{}
}

// Destroy cleans up the message.
func (fm *FileMessage) Destroy() {}

// GetBytes returns the bytes of the chunk.
func (fm *FileMessage) GetBytes() []byte {
	return fm.Chunk
}

//////////////
//  READER  //
//////////////

type fileReaderConfig struct {
	filePath        string
	chunkSize       int
	checkChunkDelim bool
	chunkDelim      byte
	maxChunkSize    int
	closeDebounce   time.Duration
	forceReRead     bool
}

type fileReaderState uint8

const (
	fileReaderStateIdle fileReaderState = iota
	fileReaderStateStarted
	fileReaderStatePaused
	fileReaderStateClosed
)

type fileReader struct {
	tel *internal.Telemetry

	fanIn *pool.FanIn[*msg[*FileMessage]]

	cfg *fileReaderConfig

	mux   *sync.RWMutex
	state fileReaderState

	file       *os.File
	fileOffset int64
	eofTimer   *time.Timer

	wakeCh chan struct{}
	wg     *sync.WaitGroup

	sourceMetrics *fileSourceMetrics
}

func newFileReader(
	tel *internal.Telemetry, fanIn *pool.FanIn[*msg[*FileMessage]], sourceMetrics *fileSourceMetrics, cfg *fileReaderConfig,
) (*fileReader, error) {

	file, err := os.Open(cfg.filePath)
	if err != nil {
		return nil, err
	}

	fr := &fileReader{
		tel: tel,

		fanIn: fanIn,

		cfg: cfg,

		mux:   &sync.RWMutex{},
		state: fileReaderStateIdle,

		file:       file,
		fileOffset: 0,
		eofTimer:   time.NewTimer(cfg.closeDebounce),

		wakeCh: make(chan struct{}),
		wg:     &sync.WaitGroup{},

		sourceMetrics: sourceMetrics,
	}

	return fr, nil
}

// start starts a new reader goroutine if the reader was idle,
// re-open the file and start the reader goroutine if the reader was closed,
// and notifies the reader goroutine if the reader was paused.
func (fr *fileReader) start(ctx context.Context) error {
	fr.mux.Lock()
	defer fr.mux.Unlock()

	switch fr.state {
	case fileReaderStateIdle:
		go fr.read(ctx)

	case fileReaderStatePaused:
		select {
		case fr.wakeCh <- struct{}{}:
		default:
		}

	case fileReaderStateClosed:
		file, err := os.Open(fr.cfg.filePath)
		if err != nil {
			return err
		}

		fr.file = file
		go fr.read(ctx)

	default:
		return nil
	}

	fr.state = fileReaderStateStarted

	fr.sourceMetrics.incrementActiveReaders()

	return nil
}

func (fr *fileReader) read(ctx context.Context) {
	defer fr.close()

	fr.wg.Add(1)
	defer fr.wg.Done()

	fr.tel.LogInfo("reading file", "path", fr.cfg.filePath)

	if fr.cfg.forceReRead {
		fr.fileOffset = 0
	} else if fr.fileOffset > 0 {
		// Seek to the last read offset
		if _, err := fr.file.Seek(fr.fileOffset, io.SeekStart); err != nil {
			fr.tel.LogError("failed to seek file", err, "path", fr.cfg.filePath)
			return
		}
	}

	reader := bufio.NewReaderSize(fr.file, fr.cfg.chunkSize)
	buf := make([]byte, fr.cfg.chunkSize)

	go func() {
		<-ctx.Done()
		fr.close()
	}()

	// Pre-allocate an appendix buffer when checking for the chunk delimiter
	var appendixBuf []byte
	if fr.cfg.checkChunkDelim {
		appendixBuf = make([]byte, fr.cfg.maxChunkSize)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := reader.Read(buf)
		if err != nil {
			// Check if the error is not EOF
			if !errors.Is(err, io.EOF) {
				fr.tel.LogError("failed to read file", err)

				return
			}

			// EOF reached, pause the reader
			if fr.pause(ctx) {
				return
			}

			continue
		}

		_, span := fr.tel.NewTrace(ctx, "read file chunk")

		chunkSize := n

		// Check delimiter variables
		hasAppendix := false
		appendixSize := 0
		delimFound := false

		if fr.cfg.checkChunkDelim && n > 0 {
			// Check for the chunk delimiter symbol

			if buf[n-1] == fr.cfg.chunkDelim {
				// The delimiter is exactly at the end of the chunk
				delimFound = true

			} else {
				// Find the delimiter in the next bytes
				for i := range fr.cfg.maxChunkSize - n {
					b, err := reader.ReadByte()
					if err != nil {
						break
					}

					appendixBuf[i] = b
					appendixSize++

					if b == fr.cfg.chunkDelim {
						delimFound = true
						break
					}
				}

				// Update the chunk size if an appendix has been read
				if appendixSize > 0 {
					chunkSize += appendixSize
					hasAppendix = true
				}

				if !delimFound {
					fr.tel.LogWarn("delimiter not found in appendix", "path", fr.cfg.filePath)
				}
			}
		}

		// Copy the first buf into the chunk
		chunk := make([]byte, chunkSize)
		copy(chunk, buf[:n])

		// Copy the appendix into the chunk
		if hasAppendix {
			copy(chunk[n:], appendixBuf[:appendixSize])
		}

		// Update the offset
		readBytes := int64(chunkSize)
		fr.fileOffset += readBytes

		msgOut := fr.handleChunk(chunk, chunkSize, delimFound)

		// Add span attributes
		span.SetAttributes(
			attribute.Int("chunk_size", chunkSize),
			attribute.Bool("has_appendix", hasAppendix),
		)

		msgOut.SaveSpan(span)
		span.End()

		// Update metrics
		fr.sourceMetrics.addReadBytes(readBytes)

		if err := fr.fanIn.AddTask(msgOut); err != nil {
			msgOut.Destroy()
			fr.tel.LogError("failed to write into output connector", err)
			return
		}
	}
}

func (fr *fileReader) handleChunk(chunk []byte, chunkSize int, delimFound bool) *msg[*FileMessage] {
	fileMsg := newFileMessage()
	fileMsg.Path = fr.cfg.filePath
	fileMsg.Chunk = chunk
	fileMsg.ChunkSize = chunkSize
	fileMsg.Offset = fr.fileOffset
	fileMsg.DelimiterFound = delimFound

	msgOut := message.NewMessage(fileMsg)
	timestamp := time.Now()
	msgOut.SetReceiveTime(timestamp)
	msgOut.SetTimestamp(timestamp)

	return msgOut
}

func (fr *fileReader) close() {
	fr.mux.Lock()
	defer fr.mux.Unlock()

	if fr.state != fileReaderStateClosed && fr.file != nil {
		// Close the file and stop the timer
		fr.file.Close()
		fr.eofTimer.Stop()

		fr.state = fileReaderStateClosed

		// Wait for the reader to finish
		fr.wg.Wait()

		fr.tel.LogInfo("file closed", "path", fr.cfg.filePath)

		fr.sourceMetrics.decrementActiveReaders()
	}
}

// pause blocks the reader and returns whether the reader should be stopped.
func (fr *fileReader) pause(ctx context.Context) bool {
	fr.mux.Lock()
	fr.state = fileReaderStatePaused
	fr.mux.Unlock()

	fr.eofTimer.Reset(fr.cfg.closeDebounce)
	defer fr.eofTimer.Stop()

	select {
	case <-ctx.Done():
		return true

	case <-fr.eofTimer.C:
		return true

	case <-fr.wakeCh:
	}

	return false
}

//////////////////////
//  SOURCE METRICS  //
//////////////////////

type fileSourceMetrics struct {
	tel *internal.Telemetry

	readers       atomic.Int64
	activeReaders atomic.Int64
	readBytes     atomic.Int64
}

func newFileSourceMetrics(tel *internal.Telemetry) *fileSourceMetrics {
	return &fileSourceMetrics{
		tel: tel,
	}
}

func (fsm *fileSourceMetrics) init() {
	fsm.tel.NewUpDownCounter("readers", func() int64 { return fsm.readers.Load() })
	fsm.tel.NewUpDownCounter("active_readers", func() int64 { return fsm.activeReaders.Load() })
	fsm.tel.NewCounter("read_bytes", func() int64 { return fsm.readBytes.Load() })
}

func (fsm *fileSourceMetrics) incrementReaders() {
	fsm.readers.Add(1)
}

func (fsm *fileSourceMetrics) decrementReaders() {
	fsm.readers.Add(-1)
}

func (fsm *fileSourceMetrics) incrementActiveReaders() {
	fsm.activeReaders.Add(1)
}

func (fsm *fileSourceMetrics) decrementActiveReaders() {
	fsm.activeReaders.Add(-1)
}

func (fsm *fileSourceMetrics) addReadBytes(amount int64) {
	fsm.readBytes.Add(amount)
}

/////////////////////////////
//  SOURCE IMPLEMENTATION  //
/////////////////////////////

var _ source[*FileMessage] = (*fileSource)(nil)

type fileSource struct {
	tel *internal.Telemetry

	cfg *FileConfig

	fanIn *pool.FanIn[*msg[*FileMessage]]

	watcher *fsnotify.Watcher

	readers map[string]*fileReader

	metrics *fileSourceMetrics
}

func newFileSource() *fileSource {
	return &fileSource{
		fanIn: pool.NewFanIn[*msg[*FileMessage]](512),

		readers: make(map[string]*fileReader),
	}
}

func (fs *fileSource) setTelemetry(tel *internal.Telemetry) {
	fs.tel = tel
}

func (fs *fileSource) init(cfg *FileConfig) error {
	fs.cfg = cfg

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	// Add the directories to watch
	for _, dirPath := range cfg.WatchedDirs {
		if err := watcher.Add(dirPath); err != nil {
			return err
		}
	}

	fs.watcher = watcher

	// Initialize the metrics
	fs.metrics = newFileSourceMetrics(fs.tel)
	fs.metrics.init()

	return nil
}

// readExistingFiles reads all the existing files in the watched directories.
// Thi is needed because the watcher does not fire events for existing files.
func (fs *fileSource) readExistingFiles(ctx context.Context) {
	for _, dirPath := range fs.cfg.WatchedDirs {
		files, err := os.ReadDir(dirPath)
		if err != nil {
			fs.tel.LogError("failed to read directory", err)
			continue
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			path := filepath.Join(dirPath, file.Name())

			fs.addAndStartReader(ctx, path)
		}
	}
}

func (fs *fileSource) hasReader(path string) bool {
	_, ok := fs.readers[path]
	return ok
}

func (fs *fileSource) addReader(path string) error {
	reader, err := newFileReader(fs.tel, fs.fanIn, fs.metrics, fs.cfg.toReaderConfig(path))

	if err != nil {
		return err
	}

	fs.readers[path] = reader

	fs.metrics.incrementReaders()

	return nil
}

func (fs *fileSource) removeReader(filePath string) {
	reader := fs.readers[filePath]
	reader.close()

	delete(fs.readers, filePath)

	fs.metrics.decrementReaders()
}

func (fs *fileSource) startReader(ctx context.Context, path string) error {
	reader := fs.readers[path]
	return reader.start(ctx)
}

func (fs *fileSource) addAndStartReader(ctx context.Context, path string) {
	if err := fs.addReader(path); err != nil {
		fs.tel.LogError("failed to add reader", err, "path", path)
		return
	}

	if err := fs.startReader(ctx, path); err != nil {
		fs.tel.LogError("failed to start reader", err, "path", path)
	}
}

func (fs *fileSource) runBridge(ctx context.Context, outConn msgConn[*FileMessage]) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
		}

		msgOut, err := fs.fanIn.ReadTask()
		if err != nil {
			continue
		}

		if err := outConn.Write(msgOut); err != nil {
			msgOut.Destroy()
			fs.tel.LogError("failed to write into output connector", err)
		}
	}
}

func (fs *fileSource) run(ctx context.Context, outConn msgConn[*FileMessage]) {
	go fs.runBridge(ctx, outConn)

	// Before starting the watcher, read all the existing files
	fs.readExistingFiles(ctx)

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-fs.watcher.Events:
			if !ok {
				return
			}

			fs.handleEvent(ctx, event)

		case err, ok := <-fs.watcher.Errors:
			if !ok {
				return
			}

			fs.tel.LogError("watcher error", err)
		}
	}
}

func (fs *fileSource) handleEvent(ctx context.Context, event fsnotify.Event) {
	path := event.Name

	// Handle file deletion/renaming
	if event.Op&fsnotify.Remove == fsnotify.Remove ||
		event.Op&fsnotify.Rename == fsnotify.Rename {

		if fs.hasReader(path) {
			fs.removeReader(path)
		}

		return
	}

	// Handle file creation
	if event.Op&fsnotify.Create == fsnotify.Create {
		if fs.hasReader(path) {
			fs.startReader(ctx, path)
		} else {
			fs.addAndStartReader(ctx, path)
		}

		return
	}

	// Handle file modification
	if event.Op&fsnotify.Write == fsnotify.Write {
		if fs.hasReader(path) {
			fs.startReader(ctx, path)
		} else {
			fs.addAndStartReader(ctx, path)
		}

		return
	}
}

func (fs *fileSource) close() {
	for _, reader := range fs.readers {
		reader.close()
	}

	fs.watcher.Close()
}

/////////////
//  STAGE  //
/////////////

// FileStage is an ingress stage that reads file from a list of directories.
type FileStage struct {
	*stage[*FileMessage, *FileConfig]

	source *fileSource
}

// NewFileStage returns a new file ingress stage.
func NewFileStage(outputConnector msgConn[*FileMessage], cfg *FileConfig) *FileStage {
	source := newFileSource()

	return &FileStage{
		stage: newStage("file", source, outputConnector, cfg),

		source: source,
	}
}

// Init initializes the stage.
func (fs *FileStage) Init(ctx context.Context) error {
	if err := fs.source.init(fs.cfg); err != nil {
		return err
	}

	return fs.stage.Init(ctx)
}

// Run runs the stage.
func (fs *FileStage) Run(ctx context.Context) {
	fs.source.run(ctx, fs.outputConnector)
}

// Close closes the stage.
func (fs *FileStage) Close() {
	fs.source.close()
	fs.stage.Close()
}
