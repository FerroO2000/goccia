package rob

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_bitmap(t *testing.T) {
	assert := assert.New(t)

	b := newBitmap(20)
	assert.Len(b.bits, 3)

	b.set(0)
	b.set(1)
	b.set(17)
	b.set(18)
	assert.Equal(byte(0b11000000), b.bits[0])
	assert.Equal(byte(0), b.bits[1])
	assert.Equal(byte(0b01100000), b.bits[2])

	assert.True(b.isSet(0))
	assert.True(b.isSet(1))
	assert.True(b.isSet(17))
	assert.True(b.isSet(18))
	assert.False(b.isSet(19))

	b.reset()
	assert.Equal(byte(0), b.bits[0])
	assert.Equal(byte(0), b.bits[1])
	assert.Equal(byte(0), b.bits[2])
}
