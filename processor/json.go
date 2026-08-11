package processor

import (
	"encoding/json"
	"errors"
	"io"
	"strings"

	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// ─── Message ────────────────────────────────────────────────────────────────|

// JSONMessage carries a typed value between JSON processor stages.
type JSONMessage[T any] struct {
	// Data is the decoded value or the value to encode.
	Data T
}

func newJSONMessage[T any](data T) *JSONMessage[T] {
	return &JSONMessage[T]{
		Data: data,
	}
}

// Destroy releases resources owned by the message. JSONMessage owns no
// external resources, so Destroy is a no-op.
func (m *JSONMessage[T]) Destroy() {}

// ─── Errors ─────────────────────────────────────────────────────────────────|

var (
	errJSONInputTooLarge = errors.New("JSON input exceeds maximum size")
	errJSONNullRejected  = errors.New("top-level JSON null is not allowed")
	errJSONTrailingValue = errors.New("JSON input contains multiple top-level values")
)

const (
	jsonErrorTypeInputTooLarge   = "goccia.json.input_too_large"
	jsonErrorTypeNullRejected    = "goccia.json.null_rejected"
	jsonErrorTypeTrailingValue   = "goccia.json.trailing_value"
	jsonErrorTypeSyntaxError     = "goccia.json.syntax_error"
	jsonErrorTypeTypeError       = "goccia.json.type_error"
	jsonErrorTypeUnknownField    = "goccia.json.unknown_field"
	jsonErrorTypeUnsupportedType = "goccia.json.unsupported_type"
	jsonErrorTypeUnsupportedVal  = "goccia.json.unsupported_value"
	jsonErrorTypeMarshalerError  = "goccia.json.marshaler_error"
)

// getJSONErrorType returns a predictable, low-cardinality error.type value.
// An empty value denotes a successful operation.
func getJSONErrorType(err error) string {
	if err == nil {
		return ""
	}

	switch {
	case errors.Is(err, errJSONInputTooLarge):
		return jsonErrorTypeInputTooLarge
	case errors.Is(err, errJSONNullRejected):
		return jsonErrorTypeNullRejected
	case errors.Is(err, errJSONTrailingValue):
		return jsonErrorTypeTrailingValue
	}

	var marshalerErr *json.MarshalerError
	if errors.As(err, &marshalerErr) {
		return jsonErrorTypeMarshalerError
	}

	var unsupportedTypeErr *json.UnsupportedTypeError
	if errors.As(err, &unsupportedTypeErr) {
		return jsonErrorTypeUnsupportedType
	}

	var unsupportedValueErr *json.UnsupportedValueError
	if errors.As(err, &unsupportedValueErr) {
		return jsonErrorTypeUnsupportedVal
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return jsonErrorTypeSyntaxError
	}

	var unmarshalTypeErr *json.UnmarshalTypeError
	if errors.As(err, &unmarshalTypeErr) {
		return jsonErrorTypeTypeError
	}

	// DisallowUnknownFields returns a formatted error without an exported,
	// distinguishable error type in encoding/json.
	if strings.HasPrefix(err.Error(), "json: unknown field ") {
		return jsonErrorTypeUnknownField
	}

	return semconv.ErrorTypeOther.Value.AsString()
}
