package kvraft

func min(x, y int32) int32 {
	if x <= y {
		return x
	}
	return y
}

func max(x, y int32) int32 {
	if x >= y {
		return x
	}
	return y
}
