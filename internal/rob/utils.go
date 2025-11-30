package rob

func getSeqNumDistance(curr, against, maxSeqNum uint64) uint64 {
	if curr >= against {
		return curr - against
	}
	return maxSeqNum + curr + 1 - against
}
