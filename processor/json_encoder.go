package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"github.com/FerroO2000/goccia/processor/metrics"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

const (
	// DefaultJSONEncoderConfigEscapeHTML enables escaping of HTML-sensitive
	// characters in JSON strings.
	DefaultJSONEncoderConfigEscapeHTML = true
)

// JSONEncoderConfig configures a [JSONEncoderStage]. Encoder-specific settings
// are captured when the stage is initialized.
type JSONEncoderConfig struct {
	*config.Base

	// Indent enables pretty printing when non-empty.
	// Typical values are "  " or "\t".
	// It should contain only JSON whitespace to keep the output valid JSON.
	Indent string

	// IndentPrefix is written at the beginning of indented lines.
	// It is ignored when Indent is empty.
	// It should contain only JSON whitespace to keep the output valid JSON.
	IndentPrefix string

	// EscapeHTML escapes <, > and & as JSON Unicode sequences.
	EscapeHTML bool
}

// NewJSONEncoderConfig returns the default JSON encoder configuration.
func NewJSONEncoderConfig(runningMode config.StageRunningMode) *JSONEncoderConfig {
	return &JSONEncoderConfig{
		Base: config.NewBase(runningMode),

		EscapeHTML: DefaultJSONEncoderConfigEscapeHTML,
	}
}

// Validate checks the JSON encoder configuration.
func (c *JSONEncoderConfig) Validate(ac *config.AnomalyCollector) {
	c.Base.Validate(ac)
}

// ─── Message ────────────────────────────────────────────────────────────────|

var _ msgSer = (*JSONEncodedMessage)(nil)

// JSONEncodedMessage contains one JSON-encoded value.
type JSONEncodedMessage struct {
	// Data contains the encoded JSON bytes without a trailing newline.
	Data []byte
}

func newJSONEncodedMessage(data []byte) *JSONEncodedMessage {
	return &JSONEncodedMessage{
		Data: data,
	}
}

// Destroy releases resources owned by the message. JSONEncodedMessage owns no
// external resources, so Destroy is a no-op.
func (m *JSONEncodedMessage) Destroy() {}

// GetBytes returns Data without copying it. Callers must not mutate the
// returned slice while the message is in use.
func (m *JSONEncodedMessage) GetBytes() []byte {
	return m.Data
}

// ─── Encoder ────────────────────────────────────────────────────────────────|

type jsonEncoderConfig struct {
	indent       string
	indentPrefix string
	escapeHTML   bool
}

type jsonEncoderMode int8

const (
	jsonEncoderModeDefault jsonEncoderMode = iota
	jsonEncoderModeIndent
	jsonEncoderModeConfigured
)

func (em jsonEncoderMode) fromConfig(config jsonEncoderConfig) jsonEncoderMode {
	if !config.escapeHTML {
		return jsonEncoderModeConfigured
	}

	if config.indent != "" {
		return jsonEncoderModeIndent
	}

	return jsonEncoderModeDefault
}

type jsonEncoder[T any] struct {
	config jsonEncoderConfig

	mode jsonEncoderMode
}

func newJSONEncoder[T any](config jsonEncoderConfig) *jsonEncoder[T] {
	return &jsonEncoder[T]{
		config: config,

		mode: jsonEncoderModeDefault.fromConfig(config),
	}
}

func (e *jsonEncoder[T]) encode(dataIn T) ([]byte, error) {
	switch e.mode {
	case jsonEncoderModeIndent:
		return e.encodeWithIndent(dataIn)

	case jsonEncoderModeConfigured:
		return e.encodeWithBuffer(dataIn)

	default:
		return json.Marshal(dataIn)
	}
}

func (e *jsonEncoder[T]) encodeWithIndent(dataIn T) ([]byte, error) {
	return json.MarshalIndent(dataIn, e.config.indentPrefix, e.config.indent)
}

func (e *jsonEncoder[T]) encodeWithBuffer(dataIn T) ([]byte, error) {
	buf := &bytes.Buffer{}

	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(e.config.escapeHTML)

	if e.config.indent != "" {
		enc.SetIndent(e.config.indentPrefix, e.config.indent)
	}

	if err := enc.Encode(dataIn); err != nil {
		return nil, err
	}

	// Encode always appends a newline after a successful encoding,
	// so it is necessary to trim it
	data := buf.Bytes()

	return data[:len(data)-1], nil
}

// ─── Environment ────────────────────────────────────────────────────────────|

type jsonEncoderEnv[T any] struct {
	*env.BaseEnv[*JSONEncoderConfig, *metrics.JsonEncoder]

	encoder *jsonEncoder[T]
}

func newJSONEncoderEnv[T any](config *JSONEncoderConfig) *jsonEncoderEnv[T] {
	return &jsonEncoderEnv[T]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewJsonEncoder()),
	}
}

func (e *jsonEncoderEnv[T]) Init(ctx context.Context) error {
	if err := e.BaseEnv.Init(ctx); err != nil {
		return err
	}

	e.encoder = newJSONEncoder[T](jsonEncoderConfig{
		indent:       e.Config.Indent,
		indentPrefix: e.Config.IndentPrefix,
		escapeHTML:   e.Config.EscapeHTML,
	})

	return nil
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type jsonEncoderWorker[T any] struct {
	worker.BaseWorker[*jsonEncoderEnv[T]]
}

func newJSONEncoderWorkerMaker[T any]() func() *jsonEncoderWorker[T] {
	return func() *jsonEncoderWorker[T] {
		return &jsonEncoderWorker[T]{}
	}
}

func (w *jsonEncoderWorker[T]) Handle(ctx context.Context, msgIn *msg[*JSONMessage[T]]) (*msg[*JSONEncodedMessage], error) {
	_, span := w.Tel.StartTrace(ctx, "encode json data")
	defer span.End()

	encStartTime := time.Now()

	inputData := msgIn.GetBody().Data
	data, err := w.Env.encoder.encode(inputData)

	encDuration := time.Since(encStartTime).Seconds()
	errType := getJSONErrorType(err)

	// TODO! fix error type to not be included on success
	w.Env.Metrics.RecordGocciaJsonEncoderOperationDuration(ctx, encDuration, errType)

	if err != nil {
		return nil, err
	}

	w.Env.Metrics.RecordGocciaJsonEncoderOutputSize(ctx, int64(len(data)))

	jsonEncMsg := newJSONEncodedMessage(data)
	msgOut := message.NewMessage(jsonEncMsg)

	msgOut.SaveSpan(span)

	return msgOut, nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*JSONEncoderStage[any])(nil)

// JSONEncoderStage encodes the Data field of each [JSONMessage] as JSON.
type JSONEncoderStage[T any] struct {
	*stage.ProcessorStage[*JSONMessage[T], *JSONEncodedMessage, *jsonEncoderEnv[T]]
}

// NewJSONEncoderStage returns a JSON encoder stage that reads from inConnector
// and writes encoded messages to outConnector. Config must be non-nil.
func NewJSONEncoderStage[T any](
	inConnector msgConn[*JSONMessage[T]], outConnector msgConn[*JSONEncodedMessage], config *JSONEncoderConfig,
) *JSONEncoderStage[T] {

	env := newJSONEncoderEnv[T](config)

	return &JSONEncoderStage[T]{
		ProcessorStage: stage.NewProcessorStage(
			"json_encoder", inConnector, outConnector, env, newJSONEncoderWorkerMaker[T](), config.Stage,
		),
	}
}
