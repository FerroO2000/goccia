package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/FerroO2000/goccia/internal/config"
	"github.com/FerroO2000/goccia/internal/message"
	"github.com/FerroO2000/goccia/internal/stage"
	"github.com/FerroO2000/goccia/internal/stage/env"
	"github.com/FerroO2000/goccia/internal/stage/worker"
	"github.com/FerroO2000/goccia/processor/metrics"
)

// ─── Errors ─────────────────────────────────────────────────────────────────|

var (
	// ErrJSONInputTooLarge is returned when the input size exceeds the maximum
	// allowed size.
	ErrJSONInputTooLarge = errors.New("JSON input exceeds maximum size")

	// ErrJSONNullRejected is returned when a top-level JSON null value is
	// rejected.
	ErrJSONNullRejected = errors.New("top-level JSON null is not allowed")

	// ErrJSONTrailingValue is returned when the input contains multiple top-level
	// values.
	ErrJSONTrailingValue = errors.New("JSON input contains multiple top-level values")
)

// ─── Config ─────────────────────────────────────────────────────────────────|

const (
	// DefaultJSONDecoderConfigDisallowUnknownFields permits JSON object members
	// that do not match fields in a struct destination.
	DefaultJSONDecoderConfigDisallowUnknownFields = false

	// DefaultJSONDecoderConfigUseNumber decodes numbers stored in interface
	// values as float64 values.
	DefaultJSONDecoderConfigUseNumber = false

	// DefaultJSONDecoderConfigMaxInputBytes disables the decoder input-size
	// limit.
	DefaultJSONDecoderConfigMaxInputBytes = 0

	// DefaultJSONDecoderConfigRejectNull permits a top-level JSON null value.
	DefaultJSONDecoderConfigRejectNull = false
)

// JSONDecoderConfig configures a [JSONDecoderStage]. Decoder-specific settings
// are captured when the stage is initialized.
type JSONDecoderConfig struct {
	*config.Base

	// DisallowUnknownFields rejects object keys that do not match an exported,
	// non-ignored field when an object is decoded into a struct. Objects decoded
	// into map or interface values are unaffected.
	DisallowUnknownFields bool

	// UseNumber decodes numbers stored in interface values as [json.Number]
	// instead of float64. Typed numeric fields are unaffected.
	UseNumber bool

	// MaxInputBytes is the maximum accepted raw input size in bytes, including
	// leading and trailing whitespace.
	// A value of zero disables the limit.
	// Negative values are reset to zero during validation.
	MaxInputBytes int

	// RejectNull rejects a top-level JSON null value. Nested null values are
	// unaffected.
	RejectNull bool
}

// NewJSONDecoderConfig returns the default JSON decoder configuration.
func NewJSONDecoderConfig(runningMode config.StageRunningMode) *JSONDecoderConfig {
	return &JSONDecoderConfig{
		Base: config.NewBase(runningMode),

		DisallowUnknownFields: DefaultJSONDecoderConfigDisallowUnknownFields,
		UseNumber:             DefaultJSONDecoderConfigUseNumber,
		MaxInputBytes:         DefaultJSONDecoderConfigMaxInputBytes,
		RejectNull:            DefaultJSONDecoderConfigRejectNull,
	}
}

// Validate checks the JSON decoder configuration.
func (c *JSONDecoderConfig) Validate(ac *config.AnomalyCollector) {
	c.Base.Validate(ac)

	config.CheckNotNegative(
		ac, "MaxInputBytes", &c.MaxInputBytes, DefaultJSONDecoderConfigMaxInputBytes,
	)
}

// ─── Decoder ────────────────────────────────────────────────────────────────|

type jsonDecoderConfig struct {
	disallowUnknownFields bool
	useNumber             bool
	maxInputBytes         int
	rejectNull            bool
}

type jsonDecoderMode int8

const (
	jsonDecoderModeDefault jsonDecoderMode = iota
	jsonDecoderModeConfigured
)

func (dm jsonDecoderMode) fromConfig(config jsonDecoderConfig) jsonDecoderMode {
	if config.disallowUnknownFields || config.useNumber {
		return jsonDecoderModeConfigured
	}

	return jsonDecoderModeDefault
}

type jsonDecoder[T any] struct {
	config jsonDecoderConfig

	mode jsonDecoderMode
}

func newJSONDecoder[T any](config jsonDecoderConfig) *jsonDecoder[T] {
	return &jsonDecoder[T]{
		config: config,

		mode: jsonDecoderModeDefault.fromConfig(config),
	}
}

func (d *jsonDecoder[T]) checkDataSize(data []byte) error {
	if len(data) > d.config.maxInputBytes {
		return fmt.Errorf(
			"%w: got %d bytes, maximum is %d bytes",
			ErrJSONInputTooLarge, len(data), d.config.maxInputBytes,
		)
	}

	return nil
}

func (d *jsonDecoder[T]) decode(data []byte) (T, error) {
	var res T

	if d.config.maxInputBytes > 0 {
		if err := d.checkDataSize(data); err != nil {
			return res, err
		}
	}

	var err error
	switch d.mode {
	case jsonDecoderModeConfigured:
		err = d.decodeConfigured(data, &res)

	default:
		err = json.Unmarshal(data, &res)
	}
	if err != nil {
		return res, err
	}

	if d.config.rejectNull && d.isJSONNull(data) {
		return res, ErrJSONNullRejected
	}

	return res, nil
}

func (d *jsonDecoder[T]) decodeConfigured(data []byte, res *T) error {
	dec := json.NewDecoder(bytes.NewReader(data))

	if d.config.disallowUnknownFields {
		dec.DisallowUnknownFields()
	}

	if d.config.useNumber {
		dec.UseNumber()
	}

	if err := dec.Decode(res); err != nil {
		return err
	}

	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		if err != nil {
			return err
		}

		return ErrJSONTrailingValue
	}

	return nil
}

func (d *jsonDecoder[T]) isJSONNull(data []byte) bool {
	return bytes.Equal(bytes.Trim(data, " \t\r\n"), []byte("null"))
}

// ─── Environment ────────────────────────────────────────────────────────────|

type jsonDecoderEnv[T any] struct {
	*env.BaseEnv[*JSONDecoderConfig, *metrics.JsonDecoder]

	decoder *jsonDecoder[T]
}

func newJSONDecoderEnv[T any](config *JSONDecoderConfig) *jsonDecoderEnv[T] {
	return &jsonDecoderEnv[T]{
		BaseEnv: env.NewProcessorEnv(config, metrics.NewJsonDecoder()),
	}
}

func (e *jsonDecoderEnv[T]) Init(ctx context.Context) error {
	if err := e.BaseEnv.Init(ctx); err != nil {
		return err
	}

	e.decoder = newJSONDecoder[T](jsonDecoderConfig{
		disallowUnknownFields: e.Config.DisallowUnknownFields,
		useNumber:             e.Config.UseNumber,
		maxInputBytes:         e.Config.MaxInputBytes,
		rejectNull:            e.Config.RejectNull,
	})

	return nil
}

// ─── Worker ─────────────────────────────────────────────────────────────────|

type jsonDecoderWorker[In msgSer, Out any] struct {
	worker.BaseWorker[*jsonDecoderEnv[Out]]
}

func newJSONDecoderWorkerMaker[In msgSer, Out any]() func() *jsonDecoderWorker[In, Out] {
	return func() *jsonDecoderWorker[In, Out] {
		return &jsonDecoderWorker[In, Out]{}
	}
}

func (w *jsonDecoderWorker[In, Out]) getErrorType(err error) metrics.JsonDecoderErrorType {
	switch {
	case errors.Is(err, ErrJSONInputTooLarge):
		return metrics.JsonDecoderErrorTypeInputTooLarge
	case errors.Is(err, ErrJSONNullRejected):
		return metrics.JsonDecoderErrorTypeNullRejected
	case errors.Is(err, ErrJSONTrailingValue):
		return metrics.JsonDecoderErrorTypeTrailingValue
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return metrics.JsonDecoderErrorTypeSyntaxError
	}

	var unmarshalTypeErr *json.UnmarshalTypeError
	if errors.As(err, &unmarshalTypeErr) {
		return metrics.JsonDecoderErrorTypeTypeError
	}

	// DisallowUnknownFields returns a formatted error without an exported,
	// distinguishable error type in encoding/json.
	if strings.HasPrefix(err.Error(), "json: unknown field ") {
		return metrics.JsonDecoderErrorTypeUnknownField
	}

	return metrics.JsonDecoderErrorTypeOther
}

func (w *jsonDecoderWorker[In, Out]) handleMetrics(
	ctx context.Context, decDuration float64, inputSize int64, err error) {

	if err != nil {
		errType := w.getErrorType(err)
		w.Env.Metrics.RecordGocciaJsonDecoderOperationDurationWithErrorType(ctx, decDuration, errType)
		w.Env.Metrics.RecordGocciaJsonDecoderInputSizeWithErrorType(ctx, inputSize, errType)
		return
	}

	w.Env.Metrics.RecordGocciaJsonDecoderOperationDuration(ctx, decDuration)
	w.Env.Metrics.RecordGocciaJsonDecoderInputSize(ctx, inputSize)
}

func (w *jsonDecoderWorker[In, Out]) Handle(ctx context.Context, msgIn *msg[In]) (*msg[*JSONMessage[Out]], error) {
	_, span := w.Tel.StartTrace(ctx, "decode json data")
	defer span.End()

	decStartTime := time.Now()

	inputData := msgIn.GetBody().GetBytes()
	inputSize := len(inputData)

	decodedData, err := w.Env.decoder.decode(inputData)

	decDuration := time.Since(decStartTime).Seconds()
	w.handleMetrics(ctx, decDuration, int64(inputSize), err)

	if err != nil {
		return nil, err
	}

	jsonMsg := NewJSONMessage(decodedData)
	msgOut := message.NewMessage(jsonMsg)

	msgOut.SaveSpan(span)

	return msgOut, nil
}

// ─── Stage ──────────────────────────────────────────────────────────────────|

var _ stage.Stage = (*JSONDecoderStage[msgSer, *any])(nil)

// JSONDecoderStage decodes serialized JSON into values of type Out. Out can be
// any type supported by encoding/json. When Out is a pointer type and RejectNull
// is disabled, a top-level JSON null value produces a nil Out value.
type JSONDecoderStage[In msgSer, Out any] struct {
	*stage.ProcessorStage[In, *JSONMessage[Out], *jsonDecoderEnv[Out]]
}

// NewJSONDecoderStage returns a JSON decoder stage that reads from inConnector
// and writes decoded messages to outConnector. Config must be non-nil.
func NewJSONDecoderStage[In msgSer, Out any](
	inConnector msgConn[In], outConnector msgConn[*JSONMessage[Out]], config *JSONDecoderConfig,
) *JSONDecoderStage[In, Out] {

	env := newJSONDecoderEnv[Out](config)

	return &JSONDecoderStage[In, Out]{
		ProcessorStage: stage.NewProcessorStage(
			"json_decoder", inConnector, outConnector, env, newJSONDecoderWorkerMaker[In, Out](), config.Stage,
		),
	}
}
