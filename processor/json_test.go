package processor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getJSONErrorType(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "success", want: ""},
		{name: "input too large", err: fmt.Errorf("context: %w", errJSONInputTooLarge), want: jsonErrorTypeInputTooLarge},
		{name: "null rejected", err: errJSONNullRejected, want: jsonErrorTypeNullRejected},
		{name: "trailing value", err: errJSONTrailingValue, want: jsonErrorTypeTrailingValue},
		{name: "syntax error", err: &json.SyntaxError{}, want: jsonErrorTypeSyntaxError},
		{name: "EOF", err: io.EOF, want: jsonErrorTypeSyntaxError},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF, want: jsonErrorTypeSyntaxError},
		{name: "unmarshal type error", err: &json.UnmarshalTypeError{}, want: jsonErrorTypeTypeError},
		{name: "unknown field", err: errors.New(`json: unknown field "extra"`), want: jsonErrorTypeUnknownField},
		{name: "unsupported type", err: &json.UnsupportedTypeError{}, want: jsonErrorTypeUnsupportedType},
		{name: "unsupported value", err: &json.UnsupportedValueError{}, want: jsonErrorTypeUnsupportedVal},
		{name: "marshaler error", err: &json.MarshalerError{}, want: jsonErrorTypeMarshalerError},
		{name: "other", err: errors.New("other error"), want: "_OTHER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, getJSONErrorType(tt.err))
		})
	}
}
