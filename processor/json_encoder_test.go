package processor

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type jsonEncoderTestPayload struct {
	Text string `json:"text"`
}

type jsonEncoderUnsupportedPayload struct {
	Value func() `json:"value"`
}

func Test_jsonEncoder_New(t *testing.T) {
	tests := []struct {
		name   string
		config jsonEncoderConfig
		mode   jsonEncoderMode
	}{
		{
			name:   "default",
			config: jsonEncoderConfig{escapeHTML: true},
			mode:   jsonEncoderModeDefault,
		},
		{
			name: "prefix without indent",
			config: jsonEncoderConfig{
				indentPrefix: "\t",
				escapeHTML:   true,
			},
			mode: jsonEncoderModeDefault,
		},
		{
			name: "indent",
			config: jsonEncoderConfig{
				indent:     "  ",
				escapeHTML: true,
			},
			mode: jsonEncoderModeIndent,
		},
		{
			name:   "configured",
			config: jsonEncoderConfig{escapeHTML: false},
			mode:   jsonEncoderModeConfigured,
		},
		{
			name: "configured with indent",
			config: jsonEncoderConfig{
				indent:     "  ",
				escapeHTML: false,
			},
			mode: jsonEncoderModeConfigured,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			encoder := newJSONEncoder[jsonEncoderTestPayload](tt.config)

			assert.Equal(tt.config, encoder.config)
			assert.Equal(tt.mode, encoder.mode)
		})
	}
}

func Test_jsonEncoder_Encode(t *testing.T) {
	payload := jsonEncoderTestPayload{Text: "<tag>&"}

	tests := []struct {
		name     string
		config   jsonEncoderConfig
		expected string
	}{
		{
			name:     "default",
			config:   jsonEncoderConfig{escapeHTML: true},
			expected: `{"text":"\u003ctag\u003e\u0026"}`,
		},
		{
			name: "indent",
			config: jsonEncoderConfig{
				indent:       "  ",
				indentPrefix: "\t",
				escapeHTML:   true,
			},
			expected: "{\n" +
				"\t  \"text\": \"\\u003ctag\\u003e\\u0026\"\n" +
				"\t}",
		},
		{
			name:     "configured",
			config:   jsonEncoderConfig{escapeHTML: false},
			expected: `{"text":"<tag>&"}`,
		},
		{
			name: "configured with indent",
			config: jsonEncoderConfig{
				indent:       "  ",
				indentPrefix: "\t",
				escapeHTML:   false,
			},
			expected: "{\n" +
				"\t  \"text\": \"<tag>&\"\n" +
				"\t}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			encoder := newJSONEncoder[jsonEncoderTestPayload](tt.config)

			got, err := encoder.encode(payload)
			if !assert.NoError(err) {
				return
			}

			assert.Equal(tt.expected, string(got))
			if assert.NotEmpty(got) {
				assert.NotEqual(byte('\n'), got[len(got)-1])
			}
		})
	}
}

func Test_jsonEncoder_ConfiguredModeRemovesOnlyTerminatingNewline(t *testing.T) {
	assert := assert.New(t)
	encoder := newJSONEncoder[string](jsonEncoderConfig{escapeHTML: false})

	got, err := encoder.encode("first\nsecond")
	if !assert.NoError(err) {
		return
	}

	assert.Equal(`"first\nsecond"`, string(got))
	if assert.NotEmpty(got) {
		assert.NotEqual(byte('\n'), got[len(got)-1])
	}
}

func Test_jsonEncoder_EncodeError(t *testing.T) {
	tests := []struct {
		name   string
		config jsonEncoderConfig
	}{
		{
			name:   "default",
			config: jsonEncoderConfig{escapeHTML: true},
		},
		{
			name: "indent",
			config: jsonEncoderConfig{
				indent:     "  ",
				escapeHTML: true,
			},
		},
		{
			name:   "configured",
			config: jsonEncoderConfig{escapeHTML: false},
		},
		{
			name: "configured with indent",
			config: jsonEncoderConfig{
				indent:     "  ",
				escapeHTML: false,
			},
		},
	}

	payload := jsonEncoderUnsupportedPayload{Value: func() {}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			encoder := newJSONEncoder[jsonEncoderUnsupportedPayload](tt.config)

			got, err := encoder.encode(payload)

			assert.Nil(got)
			var unsupportedTypeError *json.UnsupportedTypeError
			assert.ErrorAs(err, &unsupportedTypeError)
		})
	}
}
