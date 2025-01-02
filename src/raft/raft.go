package raft

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
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

type Raft struct {
	// Const Raft Status
	peers       []*labrpc.ClientEnd
	persister   *Persister
	me          int32
	dead        int32
	shutdownCh  chan struct{}
	name        int32
	majority    int32
	rpcCh       chan *RPC
	lastContact int64

	newLogMu sync.Mutex
	statusMu sync.RWMutex
	// Raft Election
	role        int32
	currentTerm int32
	votedFor    int32

	// Log & Snapshot
	log         Log
	snapshot    SnapShot
	lastApplied int32
	lastLogIdx  int32
	lastLogTerm int32

	newLogCh chan struct{}
	applyCh  chan ApplyMsg

	replState  []*replicationState
	commitment *commitment

	// For Dev Debug Use
	threadGroup sync.WaitGroup
	threadCnt   int32
}

func (rf *Raft) run() {
	DPrintf("[%v]Start Raft Server", rf.getServerDetail())
	defer DPrintf("[%v]Stop Raft Server", rf.getServerDetail())
	for {
		select {
		case <-rf.shutdownCh:
			return
		default:
		}

		switch rf.getRole() {
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
const ELECTION_TIMEOUT = 350 //350ms
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
			DPrintf("[%v]Now:%d lastContact:%d %d", rf.getServerDetail(), now(), rf.lastContact, now()-rf.lastContact)
			if now()-rf.lastContact >= ELECTION_TIMEOUT {
				DPrintf("[%v]Election Timeout", rf.getServerDetail())
				rf.setRole(CANDIDATE)
				return
			}
			electTimeoutCh = electionTimer()
		case <-rf.commitment.commitCh:
			rf.applyLog()
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
				DPrintf("[%v]Newer Term[%d] Discovered, Turn To Follower",
					rf.getServerDetail(), vote.Term)
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
					return
				}
			}
		case <-electTimeoutCh:
			return
		case <-rf.commitment.commitCh:
			rf.applyLog()
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

	stepDown := make(chan struct{}, 1)
	var replState []*replicationState
	for serverNo := range rf.peers {
		rf.commitment.setMatchIdx(serverNo, 0)
		if int32(serverNo) == rf.me {
			continue
		}

		// 为每个Peer创建一个replicationState
		s := &replicationState{
			id:              serverNo,
			leaderId:        rf.me,
			currentTerm:     rf.getCurrentTerm(),
			nextIndex:       rf.getLastLogIndex() + 1,
			stopChan:        make(chan struct{}),
			stepDown:        &stepDown,
			dispatchLogChan: make(chan struct{}, 1), // Notify Thread Start Sync Log With Follower
			commitment:      rf.commitment,
		}
		replState = append(replState, s)
		rf.goFunc(func() { rf.logDispatcher(s) }, "LogDispatcher")
	}
	rf.replState = replState

	// 退出Leader状态时，关闭所有日志同步线程
	defer func() {
		for _, repl := range rf.replState {
			DPrintf("[%v]Close %d's Replication Chan", rf.getServerDetail(), repl.id)
			close(repl.stopChan)
		}
		rf.replState = nil
		DPrintf("[%v]Stop Run Leader", rf.getServerDetail())
	}()

	for rf.isLeader() {
		select {
		case <-rf.shutdownCh:
			return
		case <-stepDown:
			return
		case <-rf.newLogCh:
			for _, s := range rf.replState {
				asyncNotifyCh(s.dispatchLogChan)
			}
		case rpc := <-rf.rpcCh:
			rf.handleRPC(rpc)
		case <-rf.commitment.commitCh:
			rf.applyLog()
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
	case *InstallSnapshotRequest:
		rf.handleSnapshotRPC(args.(*InstallSnapshotRequest), reply.(*InstallSnapshotResponse))
	}
	asyncNotifyCh(rpc.replyChan)
}

func (rf *Raft) GetState() (int, bool) {
	return int(rf.getCurrentTerm()), rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.getRole() == LEADER
}

func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)

	lastLogIdx, lastLogTerm := rf.getLastLog()
	if e.Encode(rf.getCurrentTerm()) != nil ||
		e.Encode(rf.getVotedFor()) != nil ||
		e.Encode(lastLogIdx) != nil ||
		e.Encode(lastLogTerm) != nil ||
		e.Encode(rf.log.getAll()) != nil {
		log.Fatalf("[%v]Encode Raft State Failed", rf.getServerDetail())
	}

	var snapshot []byte
	if rf.snapshot.available() {
		lastIncludedIdx, lastIncludedTerm, data := rf.snapshot.getSnapshot()
		if e.Encode(int(lastIncludedIdx)) != nil ||
			e.Encode(int(lastIncludedTerm)) != nil {
			log.Fatalf("[%v]Encode Raft State Failed", rf.getServerDetail())
		}
		snapshot = data
	}

	rf.persister.Save(buf.Bytes(), snapshot)
}

func (rf *Raft) readPersist(raftState []byte, snapshot []byte) {
	if raftState == nil || len(raftState) == 0 {
		return
	}

	r := bytes.NewBuffer(raftState)
	d := labgob.NewDecoder(r)

	// Restore Log
	var logs []LogEntry
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.lastLogIdx) != nil ||
		d.Decode(&rf.lastLogTerm) != nil ||
		d.Decode(&logs) != nil {
		log.Fatalf("[%v]Read Raft State Failed", rf.getServerDetail())
	}
	rf.log.setLog(logs)

	// Restore Snapshot
	if snapshot != nil && len(snapshot) > 0 {
		var lastIncludedIndex int32
		var lastIncludedTerm int32
		if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
			log.Fatalf("[%v]Read Snanpshot Failed", rf.getServerDetail())
		}
		rf.snapshot.setSnapshot(lastIncludedIndex, lastIncludedTerm, snapshot)
	}

	DPrintf("[%v]Restore Raft, Term:%d VoteFor:%d", rf.getServerDetail(), rf.currentTerm, rf.votedFor)
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
	rf.statusMu.Lock()
	defer rf.statusMu.Unlock()
	rf.setRole(FOLLOWER)
	if term > rf.getCurrentTerm() {
		rf.setCurrentTerm(term)
		rf.setVotedFor(votedFor)
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		name:       rand.Int31() % 100,
		me:         int32(me),
		applyCh:    applyCh,
		peers:      peers,
		persister:  persister,
		shutdownCh: make(chan struct{}),
		rpcCh:      make(chan *RPC),
		commitment: &commitment{
			matchIndex: make([]int32, len(peers)),
			commitCh:   make(chan struct{}, 1),
		},
		majority: int32(len(peers) / 2),
		role:     FOLLOWER,
		votedFor: -1,
		log:      buildInMemoryLog(me),
		snapshot: &InMemorySnapshot{},
		newLogCh: make(chan struct{}, 1),
	}

	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	rf.goFunc(func() { rf.run() }, "MainLoop")
	return rf
}
