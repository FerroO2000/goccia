package processor

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type jsonDecoderTestPayload struct {
	Name   string                  `json:"name"`
	Count  int                     `json:"count"`
	Number any                     `json:"number"`
	Child  *jsonDecoderTestPayload `json:"child"`
}

type jsonDecoderTestCustom struct {
	Value string
}

func (c *jsonDecoderTestCustom) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &c.Value)
}

func Test_jsonDecoder_New(t *testing.T) {
	tests := []struct {
		name   string
		config jsonDecoderConfig
		mode   jsonDecoderMode
	}{
		{
			name: "default",
			mode: jsonDecoderModeDefault,
		},
		{
			name:   "input size limit",
			config: jsonDecoderConfig{maxInputBytes: 1},
			mode:   jsonDecoderModeDefault,
		},
		{
			name:   "reject null",
			config: jsonDecoderConfig{rejectNull: true},
			mode:   jsonDecoderModeDefault,
		},
		{
			name:   "disallow unknown fields",
			config: jsonDecoderConfig{disallowUnknownFields: true},
			mode:   jsonDecoderModeConfigured,
		},
		{
			name:   "use number",
			config: jsonDecoderConfig{useNumber: true},
			mode:   jsonDecoderModeConfigured,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			decoder := newJSONDecoder[*jsonDecoderTestPayload](tt.config)

			assert.Equal(tt.config, decoder.config)
			assert.Equal(tt.mode, decoder.mode)
		})
	}
}

func Test_jsonDecoder_Decode(t *testing.T) {
	t.Run("pointer target", func(t *testing.T) {
		assert := assert.New(t)
		decoder := newJSONDecoder[*jsonDecoderTestPayload](jsonDecoderConfig{})

		got, err := decoder.decode([]byte(`{"name":"goccia","count":1}`))
		if !assert.NoError(err) || !assert.NotNil(got) {
			return
		}

		assert.Equal("goccia", got.Name)
		assert.Equal(1, got.Count)
	})

	t.Run("pointer unmarshaler", func(t *testing.T) {
		assert := assert.New(t)
		decoder := newJSONDecoder[*jsonDecoderTestCustom](jsonDecoderConfig{})

		got, err := decoder.decode([]byte(`"custom"`))
		if !assert.NoError(err) || !assert.NotNil(got) {
			return
		}

		assert.Equal("custom", got.Value)
	})

	t.Run("null", testJSONDecoderNull)
	t.Run("unknown fields", testJSONDecoderUnknownFields)
	t.Run("numbers", testJSONDecoderNumbers)
	t.Run("input size", testJSONDecoderInputSize)
	t.Run("trailing data", testJSONDecoderTrailingData)
	t.Run("malformed input", testJSONDecoderMalformedInput)
}

func testJSONDecoderNull(t *testing.T) {
	t.Run("allowed", func(t *testing.T) {
		assert := assert.New(t)
		decoder := newJSONDecoder[*jsonDecoderTestPayload](jsonDecoderConfig{})

		got, err := decoder.decode([]byte("null"))

		assert.NoError(err)
		assert.Nil(got)
	})

	rejectingConfigs := []struct {
		name   string
		config jsonDecoderConfig
	}{
		{
			name:   "unmarshal",
			config: jsonDecoderConfig{rejectNull: true},
		},
		{
			name: "configured decoder",
			config: jsonDecoderConfig{
				rejectNull: true,
				useNumber:  true,
			},
		},
	}

	for _, tt := range rejectingConfigs {
		t.Run("rejected "+tt.name, func(t *testing.T) {
			assert := assert.New(t)
			decoder := newJSONDecoder[*jsonDecoderTestPayload](tt.config)

			for _, input := range [][]byte{[]byte("null"), []byte(" \nnull\t\r")} {
				_, err := decoder.decode(input)
				assert.ErrorIs(err, errJSONNullRejected)
			}
		})
	}

	t.Run("nested value allowed", func(t *testing.T) {
		assert := assert.New(t)
		decoder := newJSONDecoder[*jsonDecoderTestPayload](jsonDecoderConfig{rejectNull: true})

		got, err := decoder.decode([]byte(`{"name":"parent","child":null}`))
		if !assert.NoError(err) || !assert.NotNil(got) {
			return
		}

		assert.Nil(got.Child)
	})
}

func testJSONDecoderUnknownFields(t *testing.T) {
	data := []byte(`{"name":"goccia","unknown":true}`)

	t.Run("allowed", func(t *testing.T) {
		assert := assert.New(t)
		decoder := newJSONDecoder[*jsonDecoderTestPayload](jsonDecoderConfig{})

		got, err := decoder.decode(data)
		if !assert.NoError(err) || !assert.NotNil(got) {
			return
		}

		assert.Equal("goccia", got.Name)
	})

	t.Run("rejected", func(t *testing.T) {
		assert := assert.New(t)
		decoder := newJSONDecoder[*jsonDecoderTestPayload](jsonDecoderConfig{
			disallowUnknownFields: true,
		})

		_, err := decoder.decode(data)

		if assert.Error(err) {
			assert.Contains(err.Error(), "unknown field")
		}
	})
}

func testJSONDecoderNumbers(t *testing.T) {
	data := []byte(`{"number":9007199254740993}`)

	t.Run("float64", func(t *testing.T) {
		assert := assert.New(t)
		decoder := newJSONDecoder[*jsonDecoderTestPayload](jsonDecoderConfig{})

		got, err := decoder.decode(data)
		if !assert.NoError(err) || !assert.NotNil(got) {
			return
		}

		assert.IsType(float64(0), got.Number)
	})

	t.Run("json number", func(t *testing.T) {
		assert := assert.New(t)
		decoder := newJSONDecoder[*jsonDecoderTestPayload](jsonDecoderConfig{useNumber: true})

		got, err := decoder.decode(data)
		if !assert.NoError(err) || !assert.NotNil(got) {
			return
		}

		number, ok := got.Number.(json.Number)
		if !assert.True(ok) {
			return
		}

		assert.Equal("9007199254740993", number.String())
	})
}

func testJSONDecoderInputSize(t *testing.T) {
	data := []byte(" \n{\"name\":\"goccia\"}\t")

	tests := []struct {
		name        string
		config      jsonDecoderConfig
		expectsSize bool
	}{
		{
			name:   "unlimited",
			config: jsonDecoderConfig{maxInputBytes: 0},
		},
		{
			name:   "exact limit",
			config: jsonDecoderConfig{maxInputBytes: len(data)},
		},
		{
			name:        "over limit",
			config:      jsonDecoderConfig{maxInputBytes: len(data) - 1},
			expectsSize: true,
		},
		{
			name: "configured decoder over limit",
			config: jsonDecoderConfig{
				useNumber:     true,
				maxInputBytes: len(data) - 1,
			},
			expectsSize: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			decoder := newJSONDecoder[*jsonDecoderTestPayload](tt.config)

			got, err := decoder.decode(data)
			if tt.expectsSize {
				assert.ErrorIs(err, errJSONInputTooLarge)
				assert.Nil(got)
				return
			}

			if !assert.NoError(err) || !assert.NotNil(got) {
				return
			}

			assert.Equal("goccia", got.Name)
		})
	}
}

func testJSONDecoderTrailingData(t *testing.T) {
	configs := []struct {
		name   string
		config jsonDecoderConfig
	}{
		{name: "unmarshal", config: jsonDecoderConfig{}},
		{name: "configured decoder", config: jsonDecoderConfig{useNumber: true}},
	}

	invalidInputs := []struct {
		name string
		data []byte
	}{
		{name: "second object", data: []byte(`{"name":"one"} {"name":"two"}`)},
		{name: "second null", data: []byte(`{"name":"one"}null`)},
		{name: "garbage", data: []byte(`{"name":"one"}x`)},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			decoder := newJSONDecoder[*jsonDecoderTestPayload](cfg.config)

			for _, input := range invalidInputs {
				t.Run(input.name, func(t *testing.T) {
					assert := assert.New(t)

					_, err := decoder.decode(input.data)

					assert.Error(err)
				})
			}

			t.Run("whitespace", func(t *testing.T) {
				assert := assert.New(t)

				got, err := decoder.decode([]byte(" \n{\"name\":\"one\"}\t"))
				if !assert.NoError(err) || !assert.NotNil(got) {
					return
				}

				assert.Equal("one", got.Name)
			})
		})
	}
}

func testJSONDecoderMalformedInput(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty", data: nil},
		{name: "whitespace", data: []byte(" \n\t")},
		{name: "truncated object", data: []byte(`{"name":`)},
		{name: "wrong field type", data: []byte(`{"count":"one"}`)},
		{name: "numeric overflow", data: []byte(`{"count":1e100}`)},
	}

	decoder := newJSONDecoder[*jsonDecoderTestPayload](jsonDecoderConfig{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)

			_, err := decoder.decode(tt.data)

			assert.Error(err)
		})
	}
}
