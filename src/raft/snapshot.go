package raft

import "fmt"

type InstallSnapshot struct {
	Term              int32
	LeaderId          int32
	LastIncludedIndex int32
	LastIncludedTerm  int32
	Data              []byte
}

func (snapshot *InstallSnapshot) String() string {
	return fmt.Sprintf("{Term:%d LeaderId:%v LastIncludedIndex:%d LastIncludedTerm:%d}", snapshot.Term, snapshot.LeaderId, snapshot.LastIncludedIndex, snapshot.LastIncludedTerm)
}

type InstallSnapshotReply struct {
	Term int32
}

type Snapshot struct {
	LastIncludedIndex int32
	LastIncludedTerm  int32
	Data              []byte
}

func (snapshot *Snapshot) String() string {
	return fmt.Sprintf("{LastIncludedIndex:%d LastIncludedTerm:%d}", snapshot.LastIncludedIndex, snapshot.LastIncludedTerm)
}

func (rf *Raft) buildInstallSnapshot() *InstallSnapshot {
	snapshot := &InstallSnapshot{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshot.LastIncludedIndex,
		LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
		Data:              make([]byte, len(rf.snapshot.Data)),
	}
	copy(snapshot.Data, rf.snapshot.Data)
	return snapshot
}

func (rf *Raft) sendSnapshotToFollower(serverNo int, args *InstallSnapshot) int {
	rf.resetHeartbeatTimer(serverNo)

	DPrintf("[%v]Send Snapshot RPC to follower %d", rf.getServerDetail(), serverNo)
	reply := &InstallSnapshotReply{}
	ok := rf.peers[serverNo].Call("Raft.AcceptSnapshot", args, reply)
	if !ok {
		DPrintf("[%v]Send Snapshot RPC to %d timeout", rf.getServerDetail(), serverNo)
		return TIMEOUT
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		DPrintf("[%v]Follower Term > My Term, Back To Follower\n", rf.getServerDetail())
		rf.turnToFollower(reply.Term, -1)
		rf.persist()
		return ERROR
	}

	idx := args.LastIncludedIndex
	rf.nextIndex[serverNo] = max(rf.nextIndex[serverNo], idx+1)
	rf.matchIndex[serverNo] = max(rf.matchIndex[serverNo], idx)
	DPrintf("[%v]Success Send Snapshot To %d, Update NextIndex To %d, MatchIndex To %d", rf.getServerDetail(), serverNo, idx+1, idx)

	return SNAPSHOTCOMPLETE
}

func (rf *Raft) AcceptSnapshot(args *InstallSnapshot, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]Received Snapshot %v", rf.getServerDetail(), args)
	term := rf.currentTerm
	reply.Term = term

	if args.Term < term {
		return
	}

	rf.closeElectionTimer()

	if args.Term > term || rf.getRole() == CANDIDATE {
		DPrintf("[%v]Received Snapshot From %d, Term:%d > My Term:%d, Turn to follower", rf.getServerDetail(),
			args.Term, term, args.LeaderId)
		rf.turnToFollower(args.Term, args.LeaderId)
		rf.persist()
	}

	// snapshot中保存的是commitLog，所以和Commit Log一样，只能向前推进
	if rf.snapshot != nil && args.LastIncludedIndex <= rf.snapshot.LastIncludedIndex {
		return
	}

	rf.snapshot = &Snapshot{
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
	}

	idx := len(rf.log) - 1
	for ; idx >= 0; idx-- {
		if rf.log[idx].Index <= args.LastIncludedIndex {
			break
		}
	}
	rf.log = rf.log[idx+1:]
	rf.persist()
	go rf.sendCommitedLogToTester()
	DPrintf("[%v]Success Build Snapshot:%v", rf.getServerDetail(), rf.snapshot)
	DPrintf("[%v]After Build Snapshot, LastLogIdx:%d Log Length:%d", rf.getServerDetail(), rf.getLastLogIndex(), len(rf.log))
}

func (rf *Raft) sendSnapshotToTester(snapshot *Snapshot) {
	msg := ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: int(snapshot.LastIncludedIndex),
		SnapshotTerm:  int(snapshot.LastIncludedTerm),
		Snapshot:      snapshot.Data,
	}
	rf.applyCh <- msg
}

// 只有Send Command到Chan时，该方法才可能被调用
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]Build Snapshot, Index:%d, Sz:%d", rf.getServerDetail(), index, len(snapshot))

	if rf.snapshot != nil && rf.snapshot.LastIncludedIndex >= int32(index) {
		return
	}

	snap := &Snapshot{}
	snap.LastIncludedIndex = int32(index)
	snap.LastIncludedTerm = rf.getLogTermByIdx(int32(index))
	snap.Data = make([]byte, len(snapshot))
	copy(snap.Data, snapshot)

	// 把[0~index]之间的日志都删掉
	pos := rf.getLogPosByIdx(int32(index)) + 1
	rf.log = rf.log[pos:]
	rf.snapshot = snap
	rf.persist()

	DPrintf("[%v]Remove Log Before Index:%d, LastIncludedIndex:%d, LastIncludedTerm:%d, SnapSize:%d", rf.getServerDetail(), index, rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm, len(rf.snapshot.Data))
}
