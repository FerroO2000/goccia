// Package rob implements a re-order buffer.
package rob

import (
	"errors"

	"github.com/FerroO2000/goccia/connector"
)

var (
	// ErrSeqNumOutOfWindow is returned when the sequence number is out of the window.
	ErrSeqNumOutOfWindow = errors.New("sequence number out of window")
	// ErrSeqNumDuplicated is returned when the sequence number is duplicated.
	ErrSeqNumDuplicated = errors.New("sequence number duplicated")
	// ErrSeqNumTooBig is returned when the sequence number is too big.
	ErrSeqNumTooBig = errors.New("sequence number too big")
)

// EnqueueStatus is the status of the enqueue operation.
type EnqueueStatus uint8

const (
	// EnqueueStatusInOrder is returned when the item is in order,
	// so there is no need to enqueue it.
	EnqueueStatusInOrder EnqueueStatus = iota
	// EnqueueStatusPrimary is returned when the item is enqueued into the primary buffer.
	EnqueueStatusPrimary
	// EnqueueStatusAuxiliary is returned when the item is enqueued into the auxiliary buffer.
	EnqueueStatusAuxiliary
	// EnqueueStatusErr is returned when the item cannot be enqueued.
	EnqueueStatusErr
)

func (es EnqueueStatus) String() string {
	switch es {
	case EnqueueStatusInOrder:
		return "in-order"
	case EnqueueStatusPrimary:
		return "primary"
	case EnqueueStatusAuxiliary:
		return "auxiliary"
	case EnqueueStatusErr:
		return "error"
	default:
		return "unknown"
	}
}

// Config is the configuration for the re-order buffer structure [ROB].
type Config struct {
	// MaxSeqNum is the maximum possible sequence number.
	MaxSeqNum uint64

	// PrimaryBufferSize is the size of the primary buffer.
	PrimaryBufferSize uint64

	// AuxiliaryBufferSize is the size of the auxiliary buffer.
	AuxiliaryBufferSize uint64

	// FlushTreshold is the value of the fullness of the auxiliary buffer
	// needed for flushing the primary buffer.
	FlushTreshold float64

	// BaseAlpha is the base value for the alpha parameter for the EMA.
	BaseAlpha float64

	// JumpThreshold is the threshold used by the time smoother (EMA)
	// for adjusting the alpha parameter when there is a jump in the sequence.
	JumpThreshold uint64
}

type robItem interface {
	bufferItem
	timeSmootherItem
}

// ROB is an implementation of a re-order buffer.
// It uses two buffers, a primary and an auxiliary.
// The primary is automatically flushed when the auxiliary fullness
// is higher than the flush treshold.
// It uses the EMA (exponential moving average) technique to smooth and adjust
// the time associated with an item.
type ROB[T robItem] struct {
	outputConnector connector.Connector[T]

	primaryBuf   *buffer[T]
	auxiliaryBuf *buffer[T]

	flushTreshold float64

	timeSmoother *timeSmoother[T]

	isInitialized bool
}

// NewROB returns a new [ROB] (re-order buffer) with the given configuration.
func NewROB[T robItem](outputConnector connector.Connector[T], cfg *Config) *ROB[T] {
	return &ROB[T]{
		outputConnector: outputConnector,

		primaryBuf:   newBuffer[T](cfg.PrimaryBufferSize, 0, cfg.MaxSeqNum),
		auxiliaryBuf: newBuffer[T](cfg.AuxiliaryBufferSize, cfg.PrimaryBufferSize, cfg.MaxSeqNum),

		flushTreshold: cfg.FlushTreshold,

		timeSmoother: newTimeSmoother[T](cfg.BaseAlpha, cfg.JumpThreshold, cfg.MaxSeqNum),

		isInitialized: false,
	}
}

func (rob *ROB[T]) tryDequeueFromPrimary() {
	deqItems := rob.primaryBuf.dequeueConsecutives()
	deqItemCount := uint64(len(deqItems))

	if deqItemCount == 0 {
		return
	}

	for _, tmpItem := range deqItems {
		rob.deliver(tmpItem)
	}

	rob.auxiliaryBuf.transfer(rob.primaryBuf, deqItemCount)
}

func (rob *ROB[T]) enqueuePrimary(item T) (EnqueueStatus, error) {
	seqNum := item.GetSequenceNumber()

	// Check if the sequence number is out of the window
	if !rob.primaryBuf.isInRange(seqNum) {
		return EnqueueStatusErr, ErrSeqNumOutOfWindow
	}

	// Check if the sequence number is duplicated
	if rob.primaryBuf.isDuplicated(seqNum) {
		return EnqueueStatusErr, ErrSeqNumDuplicated
	}

	// Enqueue the item with the skip flag
	if !rob.primaryBuf.enqueue(item, true) {
		// The item is the next and the buffer is empty,
		// so deliver it directly and transfer the first item
		// of the auxiliary buffer into the primary
		rob.auxiliaryBuf.transfer(rob.primaryBuf, 1)
		rob.deliver(item)
		return EnqueueStatusInOrder, nil
	}

	// Dequeue and deliver consecutive items
	deqItems := rob.primaryBuf.dequeueConsecutives()

	for _, tmpItem := range deqItems {
		rob.deliver(tmpItem)
	}

	// Transfer the delivered amount of items from the auxiliary buffer
	// to the primary
	deqItemCount := uint64(len(deqItems))
	rob.auxiliaryBuf.transfer(rob.primaryBuf, deqItemCount)

	// If the delivered item count matches the window size,
	// try to dequeue consecutive items from the primary buffer
	if deqItemCount == rob.primaryBuf.size {
		rob.tryDequeueFromPrimary()
	}

	return EnqueueStatusPrimary, nil
}

func (rob *ROB[T]) enqueueAuxiliary(item T) error {
	seqNum := item.GetSequenceNumber()

	// Check if the sequence number is out of the window
	if !rob.auxiliaryBuf.isInRange(seqNum) {
		return ErrSeqNumOutOfWindow
	}

	// Check if the sequence number is duplicated
	if rob.auxiliaryBuf.isDuplicated(seqNum) {
		return ErrSeqNumDuplicated
	}

	// Always enqueue the item
	rob.auxiliaryBuf.enqueue(item, false)

	// If the auxiliary buffer is full at more than the flush treshold,
	// flush the first buffer and transfer the items
	// from the auxiliary buffer to the primary
	if rob.auxiliaryBuf.getFullness() > rob.flushTreshold {
		for _, tmpItem := range rob.primaryBuf.flush() {
			rob.deliver(tmpItem)
		}

		// Transfer the items from the auxiliary buffer
		// to the primary
		rob.primaryBuf.transfer(rob.auxiliaryBuf, rob.primaryBuf.size)

		// Try to dequeue consecutive items from the primary buffer
		rob.tryDequeueFromPrimary()
	}

	return nil
}

func (rob *ROB[T]) deliver(item T) {
	rob.timeSmoother.adjust(item)
	rob.outputConnector.Write(item)
}

// Enqueue tries to add the item into the ROB and returns the status.
// If the item is in sequence, it sends it to the output connector.
// Otherwise, it tries to add the item into the primary or the auxiliary buffer.
//
// It returns:
//   - [ErrSeqNumOutOfWindow] if the sequence number is out of the window
//     for both the buffers
//   - [ErrSeqNumDuplicated] if the sequence number is duplicated
//   - [ErrSeqNumTooBig] if the sequence number is too big
func (rob *ROB[T]) Enqueue(item T) (EnqueueStatus, error) {
	seqNum := item.GetSequenceNumber()

	if !rob.primaryBuf.isValidSize(seqNum) {
		return EnqueueStatusErr, ErrSeqNumTooBig
	}

	if !rob.isInitialized {
		rob.primaryBuf.setStartSeqNum(seqNum)

		rob.auxiliaryBuf.setStartSeqNum(seqNum)
		rob.auxiliaryBuf.incrementNextSeqNum(rob.primaryBuf.size)

		rob.isInitialized = true
	}

	status, err := rob.enqueuePrimary(item)
	if err == nil {
		return status, nil
	}

	if errors.Is(err, ErrSeqNumDuplicated) {
		return EnqueueStatusErr, err
	}

	err = rob.enqueueAuxiliary(item)
	return EnqueueStatusAuxiliary, err
}

func (rob *ROB[T]) reset() {
	rob.primaryBuf.reset()
	rob.auxiliaryBuf.reset()
	rob.timeSmoother.reset()
	rob.isInitialized = false
}

// FlushAndReset flushes the ROB and resets all the parameters.
func (rob *ROB[T]) FlushAndReset() {
	for _, item := range rob.primaryBuf.flush() {
		rob.deliver(item)
	}

	for _, item := range rob.auxiliaryBuf.flush() {
		rob.deliver(item)
	}

	rob.reset()
}
