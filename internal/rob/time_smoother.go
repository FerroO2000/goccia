package rob

import (
	"time"
)

type timeSmootherItem interface {
	GetSequenceNumber() uint64
	GetReceiveTime() time.Time
	SetTimestamp(adjustedTime time.Time)
}

type timeSmoother[T timeSmootherItem] struct {
	estimator *doubleExponentialEstimator

	prevTimestamp time.Time

	prevSeqNum uint64
	maxSeqNum  uint64
}

func newTimeSmoother[T timeSmootherItem](alpha, beta float64, maxSeqNum uint64) *timeSmoother[T] {
	return &timeSmoother[T]{
		estimator: newDoubleExponentialEstimator(alpha, beta),

		prevTimestamp: time.Time{},

		prevSeqNum: 0,
		maxSeqNum:  maxSeqNum,
	}
}

func (ts *timeSmoother[T]) adjust(item T) {
	// Extract the current receive time
	recvTime := item.GetReceiveTime()
	currValue := float64(recvTime.UnixNano())

	// Extract the current sequence number
	// and the distance to the previous one
	seqNum := item.GetSequenceNumber()
	ts.prevSeqNum = seqNum
	distance := getSeqNumDistance(seqNum, ts.prevSeqNum, ts.maxSeqNum)

	// Force the distance to be at least 1
	if distance == 0 {
		distance = 1
	}

	// Estimate the timestamp value and convert to a timestamp
	estimatedValue := ts.estimator.estimate(currValue, distance)
	currTimestamp := time.Unix(0, int64(estimatedValue))

	// Enforce monotonicity
	if currTimestamp.Before(ts.prevTimestamp) {
		currTimestamp = ts.prevTimestamp
	} else {
		ts.prevTimestamp = currTimestamp
	}

	item.SetTimestamp(currTimestamp)
}

func (ts *timeSmoother[T]) reset() {
	ts.estimator.reset()

	ts.prevSeqNum = 0
	ts.prevTimestamp = time.Time{}
}

// doubleExponentialEstimator is a double exponential estimator
// used to smooth and adjust the time associated with an item.
// The theory behind this estimator can be found here:
type doubleExponentialEstimator struct {
	alpha float64
	beta  float64

	prevLevel float64
	prevTrend float64

	estimateCount int
}

func newDoubleExponentialEstimator(alpha, beta float64) *doubleExponentialEstimator {
	return &doubleExponentialEstimator{
		alpha: alpha,
		beta:  beta,
	}
}

func (dee *doubleExponentialEstimator) estimate(value float64, n uint64) float64 {
	// Check if the value is the first in the dataset
	if dee.estimateCount == 0 {
		dee.prevLevel = value
		dee.prevTrend = 0

		dee.estimateCount++
		return value
	}

	// If it is the second, calculate the previous trend
	// by using the formula: (value - prevLevel) / n
	if dee.estimateCount == 1 {
		dee.prevTrend = (value - dee.prevLevel) / float64(n)
	}

	// Get the forecasted value based on the previous level and trend
	prevForecasted := dee.prevLevel + dee.prevTrend*float64(n)

	// Calculate the current level and trend to be used
	// by the next item
	currLevel := dee.alpha*value + (1-dee.alpha)*(prevForecasted)
	currTrend := dee.beta*(currLevel-dee.prevLevel) + (1-dee.beta)*dee.prevTrend

	dee.prevLevel = currLevel
	dee.prevTrend = currTrend

	dee.estimateCount++
	return prevForecasted
}

func (dee *doubleExponentialEstimator) reset() {
	dee.prevLevel = 0
	dee.prevTrend = 0
	dee.estimateCount = 0
}
