package raft

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func electionTimer() <-chan time.Time {
	return time.After(electionRandomTimeoutMs())
}

func electionRandomTimeoutMs() time.Duration {
	ms := 300 + (rand.Int63() % 150)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) setLastContact() {
	rf.lastContact = now()
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
func compareLog(newIdx, newTerm, oldIdx, oldTerm int32) bool {
	if oldTerm != newTerm {
		return newTerm > oldTerm
	}
	return newIdx >= oldIdx
}

//func (rf *Raft) findCommitIndex() int32 {
//	var slice []int
//	for idx, val := range rf.matchIndex {
//		if idx == int(rf.me) {
//			continue
//		}
//		slice = append(slice, int(val))
//	}
//	sort.Sort(sort.Reverse(sort.IntSlice(slice)))
//	return int32(slice[rf.majority-1])
//}

func now() int64 {
	return time.Now().UnixMilli()
}

func (rf *Raft) printGoroutineCnt() {
	defer DPrintf("[%v]stop print GoroutineCnt", rf.getServerDetail())
	for {
		select {
		case <-rf.shutdownCh:
			return
		case <-time.After(time.Second):
			fmt.Printf("当前协程数量:%d\n", runtime.NumGoroutine())
		}
	}
}

func asyncNotifyCh(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (rf *Raft) goFunc(function func(), funcName string) {
	rf.threadGroup.Add(1)
	rf.incThreadCnt()
	//DPrintf("[%v]Func %s Start, Cnt:%d", rf.getServerDetail(), funcName, rf.getThreadCnt())
	go func() {
		function()
		rf.threadGroup.Done()
		rf.decThreadCnt()
		//DPrintf("[%v]Func %s Finish, Cnt:%d", rf.getServerDetail(), funcName, rf.getThreadCnt())
	}()
}

// Needed for sorting []uint64, used to determine commitment
type int32Slice []int32

func (p int32Slice) Len() int           { return len(p) }
func (p int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Assert(condition bool, errorMsg string) {
	if !condition {
		panic(errorMsg)
	}
}
