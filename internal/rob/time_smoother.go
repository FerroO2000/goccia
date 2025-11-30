package rob

import "time"

type timeSmootherItem interface {
	GetSequenceNumber() uint64
	GetReceiveTime() time.Time
	SetTimestamp(adjustedTime time.Time)
}

type timeSmoother[T timeSmootherItem] struct {
	baseAlpha float64
	maxAlpha  float64

	jumpThreshold uint64

	lastAdjTime time.Time
	lastSeqNum  uint64

	maxSeqNum uint64
}

func newTimeSmoother[T timeSmootherItem](baseAlpha float64, jumpThreshold, maxSeqNum uint64) *timeSmoother[T] {
	return &timeSmoother[T]{
		baseAlpha: baseAlpha,
		maxAlpha:  1.0,

		jumpThreshold: jumpThreshold,

		lastAdjTime: time.Time{},
		lastSeqNum:  0,

		maxSeqNum: maxSeqNum,
	}
}

func (ts *timeSmoother[T]) getAlpha(distance uint64) float64 {
	if distance <= ts.jumpThreshold {
		return ts.baseAlpha
	}

	scale := min(float64(distance)/float64(ts.jumpThreshold), 1.0)
	return ts.baseAlpha + (ts.maxAlpha-ts.baseAlpha)*scale
}

func (ts *timeSmoother[T]) adjust(item T) {
	seqNum := item.GetSequenceNumber()
	recvTime := item.GetReceiveTime()

	if ts.lastAdjTime.IsZero() {
		item.SetTimestamp(recvTime)

		ts.lastAdjTime = recvTime
		ts.lastSeqNum = seqNum

		return
	}

	distance := getSeqNumDistance(seqNum, ts.lastSeqNum, ts.maxSeqNum)
	alpha := ts.getAlpha(distance)

	recvTimeNs := recvTime.UnixNano()
	lastAdjTimeNs := ts.lastAdjTime.UnixNano()

	adjTimeNs := alpha*float64(recvTimeNs) + (1-alpha)*float64(lastAdjTimeNs)
	adjTime := time.Unix(0, int64(adjTimeNs))

	// Enforce monotonicity, adjusted time cannot go backwards
	if adjTime.Before(ts.lastAdjTime) {
		adjTime = ts.lastAdjTime
	}

	item.SetTimestamp(adjTime)

	ts.lastAdjTime = adjTime
	ts.lastSeqNum = seqNum
}

func (ts *timeSmoother[T]) reset() {
	ts.lastAdjTime = time.Time{}
	ts.lastSeqNum = 0
}
