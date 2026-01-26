package rb

type spscBuffer[T any] struct {
	*commonBuffer

	buffer []T
}

func newSPSCBuffer[T any](capacity uint64) *spscBuffer[T] {
	return &spscBuffer[T]{
		commonBuffer: newCommonBuffer[T](capacity),

		buffer: make([]T, capacity),
	}
}

func (b *spscBuffer[T]) push(item T) bool {
	// Get head and tail
	head := b.head.Load()
	tail := b.tail.Load()

	// Check if buffer is full
	if head-tail >= b.capacity {
		// Buffer is full
		return false
	}

	// Add the item to the buffer
	itemIndex := head & b.capMask
	b.buffer[itemIndex] = item

	// Increase head
	// b.head = head + 1
	b.head.Add(1)

	return true
}

func (b *spscBuffer[T]) pop() (T, bool) {
	var zero T

	// Get head and tail
	head := b.head.Load()
	tail := b.tail.Load()

	// Check if buffer is empty
	if head == tail {
		// Buffer is empty
		return zero, false
	}

	// Get the item
	itemIndex := tail & b.capMask
	item := b.buffer[itemIndex]

	// Increase tail
	// b.tail = tail + 1
	b.tail.Add(1)

	return item, true
}
