package raft

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
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
	ms := 250 + (rand.Int63() % 150)
	return time.Duration(ms) * time.Millisecond
}

func goRoutineNum() {
	for {
		select {
		case <-time.After(time.Second / 3):
			DPrintf("GoRoutime Number: %d", runtime.NumGoroutine())
		}
	}
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

func getCurrentTime() int64 {
	return time.Now().UnixMilli()
}

func calGap(lastTime int64) int64 {
	return getCurrentTime() - lastTime
}

// 判断log1是不是和log2一样新甚至更新
func compareLog(LogIdx1, LogTerm1, LogIdx2, LogTerm2 int32) bool {
	if LogTerm1 > LogTerm2 {
		return true
	}
	if LogTerm1 == LogTerm2 &&
		LogIdx1 >= LogIdx2 {
		return true
	}
	return false
}

func boolToVote(b bool) string {
	if b {
		return "Vote"
	}
	return "Not Vote"
}
