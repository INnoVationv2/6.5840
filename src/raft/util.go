package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
	"time"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func getRandomTimeoutMs() time.Duration {
	ms := 300 + (rand.Int63() % 150)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) getRoleStr() string {
	role := rf.getRole()
	if role == LEADER {
		return "Leader   "
	} else if role == CANDIDATE {
		return "Candidate"
	} else {
		return "Follower "
	}
}

func (rf *Raft) getServerDetail() string {
	return fmt.Sprintf("%v %d %d_%d", rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me)
}

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

// 判断log1和log2是否至少一样新或者更新
func compareLog(LogIdx1, LogTerm1, LogIdx2, LogTerm2 int32) bool {
	if LogTerm1 != LogTerm2 {
		return LogTerm1 > LogTerm2
	}
	return LogIdx1 >= LogIdx2
}

func (rf *Raft) findCommitIndex() int32 {
	var slice []int
	for idx, val := range rf.matchIndex {
		if idx == int(rf.me) {
			continue
		}
		slice = append(slice, int(val))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(slice)))
	return int32(slice[rf.majority-1])
}

func getCurrentTime() int64 {
	return time.Now().UnixMilli()
}
