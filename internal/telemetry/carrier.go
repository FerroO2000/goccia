package telemetry

import (
	"slices"

	"github.com/segmentio/kafka-go"
)

const (
	textMapPropagatorFields = 1
)

// KafkaHeaderCarrier is a trace carrier that propagates the trace context
// through Kafka headers.
type KafkaHeaderCarrier struct {
	headers []kafka.Header
}

// NewKafkaHeaderCarrier creates a new KafkaHeaderCarrier.
func NewKafkaHeaderCarrier(headers []kafka.Header) *KafkaHeaderCarrier {
	h := make([]kafka.Header, 0, len(headers)+textMapPropagatorFields)
	h = append(h, headers...)

	return &KafkaHeaderCarrier{
		headers: h,
	}
}

// Get returns the value of the header with the given key.
func (khc *KafkaHeaderCarrier) Get(key string) string {
	for _, header := range khc.headers {
		if key == header.Key {
			return string(header.Value)
		}
	}
	return ""
}

// Set sets the value of the header with the given key.
func (khc *KafkaHeaderCarrier) Set(key, value string) {
	khc.headers = slices.DeleteFunc(khc.headers, func(header kafka.Header) bool {
		return header.Key == key
	})

	khc.headers = append(khc.headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

// Keys returns the list of header keys.
func (khc *KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(khc.headers))
	for _, header := range khc.headers {
		keys = append(keys, header.Key)
	}
	return keys
}

// Headers returns the list of headers.
func (khc *KafkaHeaderCarrier) Headers() []kafka.Header {
	return khc.headers
}
