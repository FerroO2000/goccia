package rob

type bufferItem interface {
	GetSequenceNumber() uint64
}

type buffer[T bufferItem] struct {
	size uint64

	items     []T
	itemCount uint64

	bitmap *bitmap

	startSeqNum uint64
	maxSeqNum   uint64
	nextSeqNum  uint64
}

func newBuffer[T bufferItem](size, startSeqNum, maxSeqNum uint64) *buffer[T] {
	return &buffer[T]{
		size: size,

		items:     make([]T, size),
		itemCount: 0,

		bitmap: newBitmap(size),

		startSeqNum: startSeqNum,
		maxSeqNum:   maxSeqNum,
		nextSeqNum:  startSeqNum,
	}
}

func (b *buffer[T]) getItemIndex(seqNum uint64) uint64 {
	return getSeqNumDistance(seqNum, b.nextSeqNum, b.maxSeqNum) % b.size
}

func (b *buffer[T]) isValidSize(seqNum uint64) bool {
	return seqNum <= b.maxSeqNum
}

func (b *buffer[T]) isInRange(seqNum uint64) bool {
	if getSeqNumDistance(seqNum, b.nextSeqNum, b.maxSeqNum) >= b.size {
		return false
	}
	return true
}

func (b *buffer[T]) isDuplicated(seqNum uint64) bool {
	return b.bitmap.isSet(b.getItemIndex(seqNum))
}

func (b *buffer[T]) incrementNextSeqNum(amount uint64) {
	b.nextSeqNum = (b.nextSeqNum + amount) % (b.maxSeqNum + 1)
}

func (b *buffer[T]) shiftLeft(amount uint64) {
	// Copy the items to the start of the buffer
	// and reset the last `amount` items
	copy(b.items, b.items[amount:])
	for i := range amount {
		b.items[b.size-i-1] = *new(T)
	}

	b.bitmap.shiftLeft(amount)
	b.incrementNextSeqNum(amount)
}

func (b *buffer[T]) insertItem(index uint64, item T) {
	b.items[index] = item
	b.bitmap.set(index)
	b.itemCount++
}

// enqueue try to insert the item into the buffer.
// When `skip` is set, it returns true if the item is actually inserted,
// meaning the dequeue method can be called. Otherwise, the item is in sequence,
// so it can be skipped (not inserted).
// If `skip` is not set, the item is always inserted.
func (b *buffer[T]) enqueue(item T, skip bool) bool {
	seqNum := item.GetSequenceNumber()

	// If the item is the next and the buffer is empty,
	// just increment the next sequence number.
	// No need to call dequeue
	if skip && seqNum == b.nextSeqNum && b.itemCount == 0 {
		b.incrementNextSeqNum(1)
		return false
	}

	itemIndex := b.getItemIndex(seqNum)
	b.insertItem(itemIndex, item)

	return true
}

// dequeueConsecutives dequeues the consecutive items in the buffer.
func (b *buffer[T]) dequeueConsecutives() []T {
	consItems := b.bitmap.getConsecutive()

	if consItems == 0 {
		return nil
	}

	items := make([]T, 0, consItems)
	for index := range consItems {
		items = append(items, b.items[index])
	}

	b.shiftLeft(consItems)
	b.itemCount -= consItems

	return items
}

// transfer moves the first `count` items from the current buffer to the `dest` buffer.
func (b *buffer[T]) transfer(dest *buffer[T], count uint64) {
	if count == 0 {
		return
	}

	// If the current buffer is empty, increment the next sequence number only
	if b.itemCount == 0 {
		b.incrementNextSeqNum(count)
		return
	}

	for index := range min(count, b.size) {
		if b.bitmap.isSet(index) {
			// Move the item into the destination buffer at the end,
			// and decrement the number of items
			dest.insertItem(dest.size-count+index, b.items[index])
			b.itemCount--
		}
	}

	// Shift the items in the current buffer
	b.shiftLeft(count)
}

func (b *buffer[T]) getFullness() float64 {
	return float64(b.itemCount) / float64(b.size)
}

func (b *buffer[T]) flush() []T {
	items := make([]T, 0, b.itemCount)

	for itemIdx, item := range b.items {
		if b.bitmap.isSet(uint64(itemIdx)) {
			items = append(items, item)
		}
	}

	clear(b.items)
	b.itemCount = 0
	b.bitmap.reset()

	b.incrementNextSeqNum(b.size)

	return items
}

func (b *buffer[T]) reset() {
	clear(b.items)
	b.itemCount = 0
	b.bitmap.reset()

	b.nextSeqNum = b.startSeqNum
}

func (b *buffer[T]) setStartSeqNum(seqNum uint64) {
	b.startSeqNum = seqNum
	b.nextSeqNum = seqNum
}
