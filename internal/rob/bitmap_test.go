package rob

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_bitmap(t *testing.T) {
	assert := assert.New(t)

	b := newBitmap(20)

	b.set(0)
	b.set(1)
	b.set(17)
	b.set(18)

	assert.True(b.isSet(0))
	assert.True(b.isSet(1))
	assert.True(b.isSet(17))
	assert.True(b.isSet(18))
	assert.False(b.isSet(19))

	b.shiftLeft(3)
	assert.True(b.isSet(14))
	assert.True(b.isSet(15))
	assert.False(b.isSet(17))
	assert.False(b.isSet(18))

	b.shiftLeft(14)
	assert.Equal(uint64(2), b.getConsecutive())

	b.set(0)
	b.set(19)
	b.reset()
	assert.False(b.isSet(0))
	assert.False(b.isSet(19))
}

func Benchmark_bitmap(b *testing.B) {
	bitmap := newBitmap(64)

	for b.Loop() {
		bitmap.set(0)
		bitmap.set(63)
		bitmap.shiftLeft(1)
	}
}
