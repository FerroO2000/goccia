package processor

// ─── Message ────────────────────────────────────────────────────────────────|

// JSONMessage carries a typed value between JSON processor stages.
type JSONMessage[T any] struct {
	// Data is the decoded value or the value to encode.
	Data T
}

// NewJSONMessage returns a new JSONMessage with the given generic data.
func NewJSONMessage[T any](data T) *JSONMessage[T] {
	return &JSONMessage[T]{
		Data: data,
	}
}

// Destroy releases resources owned by the message. JSONMessage owns no
// external resources, so Destroy is a no-op.
func (m *JSONMessage[T]) Destroy() {}
