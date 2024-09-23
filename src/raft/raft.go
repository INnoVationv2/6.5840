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

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int32
	dead      int32
	name      int32

	lastSendTime []int64

	majority     int32
	applyCh      chan ApplyMsg
	applyChMutex sync.Mutex

	role      int32
	heartbeat int32

	currentTerm int32
	votedFor    int32
	log         []LogEntry

	commitIndex int32
	lastApplied int32

	nextIndex  []int32
	matchIndex []int32

	snapshot       *Snapshot
	snapshotStatus int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.currentTerm), rf.getRole() == LEADER
}

func (rf *Raft) isLeader() bool {
	return rf.getRole() == LEADER
}

func (rf *Raft) persist() {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	if e.Encode(rf.currentTerm) != nil ||
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
		rf.lastApplied = rf.snapshot.LastIncludedIndex
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) turnToFollower(term int32, votedFor int32) {
	rf.setRole(FOLLOWER)
	rf.currentTerm = term
	rf.votedFor = votedFor
}

func (rf *Raft) turnToLeader() {
	// Leader必须由Candidate转变而来
	rf.setRole(LEADER)
	rf.votedFor = rf.me
	for idx := range rf.peers {
		rf.nextIndex[idx] = int32(len(rf.log))
		rf.matchIndex[idx] = 0
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.applyCh = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = int32(me)
	rf.name = rand.Int31() % 100
	rf.startElectionTimer()

	rf.majority = int32(len(rf.peers) / 2)
	rf.role = FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0, Index: 0, Command: nil})

	peerNum := len(peers)
	rf.lastSendTime = make([]int64, peerNum)
	rf.nextIndex = make([]int32, peerNum)
	for i := 0; i < peerNum; i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int32, peerNum)
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	go rf.ticker()
	return rf
}
