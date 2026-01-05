// Package connector contains the connector implementations
// offered by goccia.
package connector

import (
	"context"
)

// Connector is a generic connector interface.
// This can be implemented by a third-party actor that
// wants to use a custom connector.
type Connector[T any] interface {
	// Write writes an item to the connector.
	// It returns ErrClosed if the connector is closed.
	Write(item T) error

	// Read reads an item from the connector.
	// It returns ErrClosed if the connector is closed and
	// there are no items in the buffer or ctx.Err()
	// if context is canceled.
	Read(ctx context.Context) (T, error)

	// Close closes the connector.
	// After Close is called, Read and Write will return ErrClosed.
	Close()
}
