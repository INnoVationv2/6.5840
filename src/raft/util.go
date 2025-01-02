package raft

import (
	"6.5840/logger"
	"fmt"
	"math/rand"
	"runtime"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		logger.Debug(format, a...)
	}
}

func getTime() string {
	return time.Now().Format("15:04:05.000")
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

// 判断Candidate的Log是否和自己一样新或者更新
func compareLog(newIdx, newTerm, oldIdx, oldTerm int32) bool {
	if oldTerm != newTerm {
		return newTerm > oldTerm
	}
	return newIdx >= oldIdx
}

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
	//rf.threadGroup.Add(1)
	//atomic.AddInt32(&rf.threadCnt, 1)
	//fmt.Printf("[%v]Func %s Start, Cnt:%d\n", rf.getServerDetail(), funcName, atomic.LoadInt32(&rf.threadCnt))
	go func() {
		function()
		//rf.threadGroup.Done()
		//atomic.AddInt32(&rf.threadCnt, -1)
		//fmt.Printf("[%v]Func %s Finish, Cnt:%d\n", rf.getServerDetail(), funcName, atomic.LoadInt32(&rf.threadCnt))
	}()
}

// Needed for sorting []uint32, used to determine commitment
type int32Slice []int32

func (p int32Slice) Len() int           { return len(p) }
func (p int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Assert(condition bool, errorMsg string) {
	if !condition {
		panic(errorMsg)
	}
}
