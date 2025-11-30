// Package message contains the structures and interfaces for messages.
package message

// Envelope interface defines the common methods for all message envelopes.
type Envelope interface {
	// Destroy cleans up the envelope.
	Destroy()
}

// Serializable interface defines the common methods for all message envelopes
// that can be serialized into bytes.
type Serializable interface {
	Envelope

	// GetBytes returns the bytes of the message.
	GetBytes() []byte
}

// ReOrderable interface defines the common methods for all re-orderable message envelopes.
// This is used in the context of the re-order buffer.
type ReOrderable interface {
	Envelope

	// GetSequenceNumber returns the sequence number of the message.
	GetSequenceNumber() uint64
}
