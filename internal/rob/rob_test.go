package rob

import (
	"context"
	"testing"
	"time"

	"github.com/FerroO2000/goccia/connector"
)

var _ robItem = (*testItem)(nil)

type testItem struct {
	seqNum    uint64
	recvTime  time.Time
	timestamp time.Time
}

func newTestItem(seqNum uint64) *testItem {
	currTime := time.Now()

	return &testItem{
		seqNum:    seqNum,
		recvTime:  currTime,
		timestamp: currTime,
	}
}

func (ti *testItem) GetSequenceNumber() uint64 {
	return ti.seqNum
}

func (ti *testItem) GetReceiveTime() time.Time {
	return ti.recvTime
}

func (ti *testItem) SetTimestamp(adjustedTime time.Time) {
	ti.timestamp = adjustedTime
}

var _ connector.Connector[*testItem] = (*testConnector)(nil)

type testConnector struct{}

func newTestConnector() *testConnector {
	return &testConnector{}
}

func (*testConnector) Write(_ *testItem) error {
	return nil
}

func (*testConnector) Read(_ context.Context) (*testItem, error) {
	return nil, nil
}

func (*testConnector) Close() {}

func Benchmark_ROB(b *testing.B) {
	totalItems := DefaultPrimaryBufferSize + DefaultAuxiliaryBufferSize

	items := make([]*testItem, 0, totalItems)
	for i := range totalItems {
		items = append(items, newTestItem(uint64(i)))
	}

	rob := NewROB(newTestConnector(), NewConfig())

	itemIdx := 0
	for b.Loop() {
		rob.Enqueue(items[itemIdx])

		itemIdx++
		if itemIdx == totalItems {
			itemIdx = 0
		}
	}
}
