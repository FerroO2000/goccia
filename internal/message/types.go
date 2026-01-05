// Package message contains the structures and interfaces for messages.
package message

// Body interface defines the common methods for all message bodies.
type Body interface {
	// Destroy cleans up the body data.
	Destroy()
}

// Serializable interface defines the common methods for all message envelopes
// that can be serialized into bytes.
type Serializable interface {
	Body

	// GetBytes returns the bytes of the message.
	GetBytes() []byte
}

// ReOrderable interface defines the common methods for all re-orderable message envelopes.
// This is used in the context of the re-order buffer.
type ReOrderable interface {
	Body

	// GetSequenceNumber returns the sequence number of the message.
	GetSequenceNumber() uint64
}
