package message

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type messageEnvelope[T Envelope] struct {
	value T
	refs  atomic.Int32
}

func newMessageEnvelope[T Envelope](value T) *messageEnvelope[T] {
	return &messageEnvelope[T]{
		value: value,
	}
}

// Message is the base struct for all messages.
// It is the data structure passed between stages.
type Message[T Envelope] struct {
	receiveTime    time.Time
	timestamp      time.Time
	sequenceNumber uint64
	isDropped      bool
	span           trace.SpanContext

	envelope *messageEnvelope[T]
}

// NewMessage creates a new message.
func NewMessage[T Envelope](value T) *Message[T] {
	return &Message[T]{
		envelope: newMessageEnvelope(value),
	}
}

// SetReceiveTime sets the time the message was received.
func (m *Message[T]) SetReceiveTime(receiveTime time.Time) {
	m.receiveTime = receiveTime
}

// GetReceiveTime returns the time the message was received.
func (m *Message[T]) GetReceiveTime() time.Time {
	return m.receiveTime
}

// SetTimestamp sets the timestamp of the message.
func (m *Message[T]) SetTimestamp(timestamp time.Time) {
	m.timestamp = timestamp
}

// GetTimestamp returns the timestamp of the message.
// It may be different from the receive time.
func (m *Message[T]) GetTimestamp() time.Time {
	return m.timestamp
}

// GetSequenceNumber returns the sequence number of the message.
// This is used in the context of the re-order buffer.
func (m *Message[T]) GetSequenceNumber() uint64 {
	return m.sequenceNumber
}

// SetSequenceNumber sets the sequence number of the message.
// This is used in the context of the re-order buffer.
func (m *Message[T]) SetSequenceNumber(sequenceNumber uint64) {
	m.sequenceNumber = sequenceNumber
}

// Drop marks the message as dropped.
func (m *Message[T]) Drop() {
	m.isDropped = true
}

// IsDropped states whether the message was dropped.
func (m *Message[T]) IsDropped() bool {
	return m.isDropped
}

// SaveSpan saves the trace span for the message.
func (m *Message[T]) SaveSpan(span trace.Span) {
	m.span = span.SpanContext()
}

// LoadSpanContext loads the trace of the message
// into the provided context.
func (m *Message[T]) LoadSpanContext(ctx context.Context) context.Context {
	return trace.ContextWithSpanContext(ctx, m.span)
}

// Clone clones the message.
func (m *Message[T]) Clone() *Message[T] {
	m.envelope.refs.Add(1)

	return &Message[T]{
		receiveTime: m.receiveTime,
		timestamp:   m.receiveTime,
		span:        m.span,
		envelope:    m.envelope,
	}
}

// Destroy cleans up the message.
// If the reference count was 0 before this method is called,
// it will call the underlying message envelopr's Destroy method.
// This lets the stage's specific data be cleaned up properly even
// if the are pooled.
func (m *Message[T]) Destroy() {
	if m.envelope.refs.Add(-1) == -1 {
		m.envelope.value.Destroy()
	}
}

// GetEnvelope returns the envelope of the message,
// i.e. the stage's specific data.
func (m *Message[T]) GetEnvelope() T {
	return m.envelope.value
}
