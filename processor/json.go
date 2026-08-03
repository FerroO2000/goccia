package processor

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/metrics"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
)

// ─── Config ─────────────────────────────────────────────────────────────────|

const (
	DefaultJSONEncoderConfigEscapeHTML = true
)

type JSONEncoderConfig struct {
	*config.Base

	// Indent enables pretty printing when non-empty.
	// Typical values are "  " or "\t".
	Indent string

	// IndentPrefix is written at the beginning of indented lines.
	// It is ignored when Indent is empty.
	IndentPrefix string

	// EscapeHTML escapes <, > and & as JSON Unicode sequences.
	EscapeHTML bool
}

func NewJSONEncoderConfig(runningMode config.StageRunningMode) *JSONEncoderConfig {
	return &JSONEncoderConfig{
		Base: config.NewBase(runningMode),

		EscapeHTML: DefaultJSONEncoderConfigEscapeHTML,
	}
}

func (c *JSONEncoderConfig) Validate(ac *config.AnomalyCollector) {
	c.Base.Validate(ac)
}

// ─── Message ────────────────────────────────────────────────────────────────|

type JSONMessage[T any] struct {
	Data T
}

func (m *JSONMessage[T]) Destroy() {}

var _ msgSer = (*JSONEncodedMessage)(nil)

type JSONEncodedMessage struct {
	Data []byte
}

func newJSONEncodedMessage(data []byte) *JSONEncodedMessage {
	return &JSONEncodedMessage{
		Data: data,
	}
}

func (m *JSONEncodedMessage) Destroy() {}

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
	jsonEncoderModeBuffer
)

type jsonEncoder[T any] struct {
	config jsonEncoderConfig

	mode jsonEncoderMode
}

func newJSONEncoder[T any](config jsonEncoderConfig) *jsonEncoder[T] {
	return &jsonEncoder[T]{
		config: config,

		mode: jsonEncoderModeDefault,
	}
}

func (e *jsonEncoder[T]) init() {
	if !e.config.escapeHTML {
		e.mode = jsonEncoderModeBuffer
		return
	}

	if e.config.indent != "" {
		e.mode = jsonEncoderModeIndent
	}
}

func (e *jsonEncoder[T]) encode(dataIn T) ([]byte, error) {
	switch e.mode {
	case jsonEncoderModeIndent:
		return e.encodeWithIndent(dataIn)

	case jsonEncoderModeBuffer:
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
	*env.BaseEnv[*JSONEncoderConfig, *metrics.EmptyMetrics]

	encoder *jsonEncoder[T]
}

func newJSONEncoderEnv[T any](config *JSONEncoderConfig) *jsonEncoderEnv[T] {
	return &jsonEncoderEnv[T]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewEmptyMetrics()),

		encoder: newJSONEncoder[T](jsonEncoderConfig{
			indent:       config.Indent,
			indentPrefix: config.IndentPrefix,
			escapeHTML:   config.EscapeHTML,
		}),
	}
}

func (e *jsonEncoderEnv[T]) Init(ctx context.Context) error {
	if err := e.BaseEnv.Init(ctx); err != nil {
		return err
	}

	e.encoder.init()

	return nil
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type jsonWorker[T any] struct {
	worker.BaseWorker[*jsonEncoderEnv[T]]
}

func newJSONEncoderWorkerMaker[T any]() func() *jsonWorker[T] {
	return func() *jsonWorker[T] {
		return &jsonWorker[T]{}
	}
}

func (w *jsonWorker[T]) Handle(ctx context.Context, msgIn *msg[*JSONMessage[T]]) (*msg[*JSONEncodedMessage], error) {
	_, span := w.Tel.StartTrace(ctx, "encode json data")
	defer span.End()

	msgBody := msgIn.GetBody()
	data, err := w.Env.encoder.encode(msgBody.Data)
	if err != nil {
		return nil, err
	}

	jsonEncMsg := newJSONEncodedMessage(data)
	msgOut := message.NewMessage(jsonEncMsg)

	msgOut.SaveSpan(span)

	return msgOut, nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*JSONEncoderStage[any])(nil)

type JSONEncoderStage[T any] struct {
	*stage.ProcessorStage[*JSONMessage[T], *JSONEncodedMessage, *jsonEncoderEnv[T]]
}

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
