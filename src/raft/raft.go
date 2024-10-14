package raft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	LEADER int32 = iota
	CANDIDATE
	FOLLOWER
)

const ELECTION_TIMEOUT = 350 //350ms

type Raft struct {
	// Raft Basic
	peers       []*labrpc.ClientEnd
	persister   *Persister
	me          int32
	dead        int32
	shutdownCh  chan struct{}
	name        int32
	majority    int32
	rpcCh       chan *RPC
	lastContact int64

	// Raft Election
	role        int32
	currentTerm int32
	votedFor    int32

	// Log & Snapshot
	log         []LogEntry
	snapshot    *Snapshot
	logMu       sync.RWMutex
	applyCh     chan ApplyMsg
	lastApplied int32
	replState   []*replicationState
	commitment  *commitment

	// For System Monitor Use
	threadGroup sync.WaitGroup
	threadCnt   int32
}

func (rf *Raft) run() {
	defer DPrintf("[%v]Stop Run", rf.getServerDetail())
	for {
		select {
		case <-rf.shutdownCh:
			return
		default:
		}

		switch rf.role {
		case LEADER:
			rf.runLeader()
		case CANDIDATE:
			rf.runCandidate()
		case FOLLOWER:
			rf.runFollower()
		}
	}
}

// Follower的功能
//
//	1.处理RPC
//	2.发送Commited Log到Tester
//	3.检查是否选举超时
func (rf *Raft) runFollower() {
	DPrintf("[%v]Run Follower", rf.getServerDetail())
	defer DPrintf("[%v]Stop Run Follower", rf.getServerDetail())

	electTimeoutCh := electionTimer()
	for {
		select {
		case <-rf.shutdownCh:
			return
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case <-electTimeoutCh:
			if now()-rf.lastContact >= ELECTION_TIMEOUT {
				DPrintf("[%v]Election Timeout", rf.getServerDetail())
				rf.setRole(CANDIDATE)
				return
			}
			electTimeoutCh = electionTimer()
		case <-rf.commitment.commitCh:
			rf.sendCommitedLogToTester()
		}
	}
}

// Candidate的功能
// 包含Follower的所有功能，除此之外：
// 向集群其他Server请求投票，如果选举超时还未收到过半选票，重新开始新一轮投票
func (rf *Raft) runCandidate() {
	DPrintf("[%v]Run Candidate", rf.getServerDetail())
	defer DPrintf("[%v]Stop Run Candidate", rf.getServerDetail())

	voteCh, voteCnt := rf.electSelf(), rf.majority
	electTimeoutCh := electionTimer()
	for rf.getRole() == CANDIDATE {
		select {
		case <-rf.shutdownCh:
			return
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case vote := <-voteCh:
			if vote.Term > rf.currentTerm {
				DPrintf("[%v]Newer term %d discovered, Turn to follower", rf.getServerDetail(), vote.Term)
				rf.setRole(FOLLOWER)
				rf.setCurrentTerm(vote.Term)
				rf.persist()
				return
			}

			if vote.VoteGranted {
				DPrintf("[%v]%d Vote", rf.getServerDetail(), vote.voterId)
				if voteCnt--; voteCnt == 0 {
					DPrintf("[%v]Get Majority Vote, Become Leader", rf.getServerDetail())
					rf.setRole(LEADER)
					rf.persist()
					return
				}
			}
		case <-electTimeoutCh:
			return
		case <-rf.commitment.commitCh:
			rf.sendCommitedLogToTester()
		}
	}
}

// Leader的功能
//
//	1.和Follower同步Log
//	2.定时向Follower发送心跳消息
//	3.处理 RPC
func (rf *Raft) runLeader() {
	DPrintf("[%v]Run Leader", rf.getServerDetail())
	defer func() {
		rf.replState = nil
		DPrintf("[%v]Stop Run Leader", rf.getServerDetail())
	}()

	rf.logMu.RLock()
	var replState []*replicationState
	for serverNo := range rf.peers {
		if int32(serverNo) == rf.me {
			continue
		}

		// 为每个Peer创建一个replicationState
		s := &replicationState{
			id:          serverNo,
			leaderId:    rf.me,
			currentTerm: rf.getCurrentTerm(),
			nextIndex:   rf.getLastLogIndex() + 1,
			stopChan:    make(chan struct{}, 1),
			triggerChan: make(chan struct{}, 1),
			commitment:  rf.commitment,
		}
		replState = append(replState, s)
		rf.goFunc(func() { rf.logDispatcher(s) }, "LogDispatcher")
	}
	rf.replState = replState
	rf.logMu.RUnlock()

	defer func() {
		for _, repl := range rf.replState {
			DPrintf("[%v]Close %d's Replication Chan", rf.getServerDetail(), repl.id)
			close(repl.stopChan)
		}
	}()

	for rf.isLeader() {
		select {
		case <-rf.shutdownCh:
			return
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case <-rf.commitment.commitCh:
			rf.sendCommitedLogToTester()
		}
	}
}

func (rf *Raft) handleRPC(rpc *RPC) {
	args, reply := rpc.req, rpc.res
	switch args.(type) {
	case *RequestVoteRequest:
		rf.handleRequestVoteRPC(args.(*RequestVoteRequest), reply.(*RequestVoteResponse))
	case *AppendEntriesRequest:
		rf.handleAppendEntriesRPC(args.(*AppendEntriesRequest), reply.(*AppendEntriesResponse))
	}
	asyncNotifyCh(rpc.replyChan)
}

func (rf *Raft) GetState() (int, bool) {
	return int(rf.getCurrentTerm()), rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.getRole() == LEADER
}

func (rf *Raft) checkRaftStatus(term int32) bool {
	return rf.killed() || !rf.isLeader() || rf.getCurrentTerm() != term
}

func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	if e.Encode(rf.getCurrentTerm()) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.role) != nil {
		log.Fatalf("[%v]Encode Raft State Failed", rf.getServerDetail())
	}

	var snapshot []byte
	if rf.snapshot != nil {
		if e.Encode(int(rf.snapshot.LastIncludedIndex)) != nil ||
			e.Encode(int(rf.snapshot.LastIncludedTerm)) != nil {
			log.Fatalf("[%v]Encode Raft State Failed", rf.getServerDetail())
		}
		snapshot = rf.snapshot.Data
	}

	rf.persister.Save(buf.Bytes(), snapshot)
}

func (rf *Raft) readPersist(raftState []byte, snapshot []byte) {
	if raftState == nil || len(raftState) == 0 {
		return
	}

	r := bytes.NewBuffer(raftState)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil ||
		d.Decode(&rf.role) != nil {
		log.Fatalf("[%v]Read Raft State Failed", rf.getServerDetail())
	}

	if snapshot != nil && len(snapshot) > 0 {
		var lastIncludedIndex int
		var lastIncludedTerm int
		if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
			log.Fatalf("[%v]Read Snanpshot Failed", rf.getServerDetail())
		}
		rf.snapshot = &Snapshot{
			LastIncludedIndex: int32(lastIncludedIndex),
			LastIncludedTerm:  int32(lastIncludedTerm),
			Data:              snapshot,
		}
	}
}

func (rf *Raft) Kill() {
	DPrintf("[%v]Kill All Thread", rf.getServerDetail())
	close(rf.shutdownCh)
	atomic.StoreInt32(&rf.dead, 1)
	//rf.threadGroup.Wait()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) turnToFollower(term int32, votedFor int32) {
	rf.setRole(FOLLOWER)
	rf.setCurrentTerm(term)
	rf.votedFor = votedFor
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.applyCh = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)
	rf.name = rand.Int31() % 100
	rf.shutdownCh = make(chan struct{}, 1)
	rf.rpcCh = make(chan *RPC)
	rf.commitment = &commitment{
		matchIndex: make([]int32, len(rf.peers)),
		commitCh:   make(chan struct{}, 1),
	}

	rf.majority = int32(len(rf.peers) / 2)
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0, Command: nil})

	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	rf.goFunc(func() { rf.run() }, "Main-Loop")
	return rf
}
