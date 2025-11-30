package rob

type bitmap struct {
	bits []byte
	num  uint64
}

func newBitmap(num uint64) *bitmap {
	return &bitmap{
		bits: make([]byte, (num/8)+1),
		num:  num,
	}
}

func (b *bitmap) set(index uint64) {
	b.bits[index/8] |= 1 << (7 - index%8)
}

func (b *bitmap) isSet(index uint64) bool {
	return b.bits[index/8]&(1<<(7-index%8)) != 0
}

func (b *bitmap) getConsecutive() uint64 {
	bits := uint64(0)

	for idx := range b.num {
		if !b.isSet(idx) {
			break
		}
		bits++
	}

	return bits
}

func (b *bitmap) shiftLeft(amount uint64) {
	if amount == 0 {
		return
	}

	bitsLen := uint64(len(b.bits))
	skipRows := amount / 8
	shiftVal := uint8(amount % 8)
	msbMask := uint8(0)
	for i := range shiftVal {
		msbMask |= 1 << (7 - i)
	}

	isFirst := true
	for byteIdx := skipRows; byteIdx < bitsLen; byteIdx++ {
		currByte := b.bits[byteIdx]

		if isFirst {
			isFirst = false
		} else if shiftVal > 0 {
			msb := currByte & msbMask
			lsb := msb >> (8 - shiftVal)
			b.bits[byteIdx-skipRows-1] |= lsb
		}

		b.bits[byteIdx-skipRows] = currByte << shiftVal
	}

	if skipRows > 0 {
		for byteIdx := bitsLen - skipRows; byteIdx < bitsLen; byteIdx++ {
			b.bits[byteIdx] = 0
		}
	}
}

func (b bitmap) reset() {
	for byteIdx := range b.bits {
		b.bits[byteIdx] = 0
	}
}
