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
	if role == Leader {
		return "Leader   "
	} else if role == Candidate {
		return "Candidate"
	} else {
		return "Follower "
	}
}

func (rf *Raft) getServerDetail() string {
	return fmt.Sprintf("%v %d %d_%d", rf.getRoleStr(), rf.getCurrentTerm(), rf.name, rf.me)
}
