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

func (rf *Raft) buildInstallSnapshot() *InstallSnapshot {
	snapshot := &InstallSnapshot{}
	snapshot.Term = rf.currentTerm
	snapshot.LeaderId = rf.me
	snapshot.LastIncludedIndex = rf.snapshot.LastIncludedIndex
	snapshot.LastIncludedTerm = rf.snapshot.LastIncludedTerm
	snapshot.Data = make([]byte, len(rf.snapshot.Data))
	copy(snapshot.Data, rf.snapshot.Data)
	return snapshot
}

func (rf *Raft) sendSnapshotToFollower(serverNo int, args *InstallSnapshot) int {
	DPrintf("[%v]Send Snapshot to follower %d", rf.getServerDetail(), serverNo)
	reply := &InstallSnapshotReply{}
	ok := rf.peers[serverNo].Call("Raft.AcceptSnapshot", args, reply)
	if !ok {
		DPrintf("[%v]Send Snapshot to %d timeout", rf.getServerDetail(), serverNo)
		return TIMEOUT
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.getRole() != LEADER || rf.getCurrentTerm() != args.Term {
		DPrintf("[%v]Send Snapshot to follower %d failed, Leader status changed:"+
			"killed:%v Role:%v Term:%d CurrentTerm:%d", rf.getServerDetail(), serverNo, rf.killed(), rf.getRoleStr(), args.Term, rf.getCurrentTerm())
		return ERROR
	}

	if reply.Term > rf.getCurrentTerm() {
		DPrintf("[%v]Follower Term > My Term, Back To Follower\n", rf.getServerDetail())
		rf.turnToFollower(reply.Term, -1)
		rf.persist()
		return ERROR
	}

	idx := args.LastIncludedIndex
	rf.setNextIndex(serverNo, max(rf.getNextIndex(serverNo), idx+1))
	rf.setMatchIndex(serverNo, max(rf.getMatchIndex(serverNo), idx))
	DPrintf("[%v]Success Send Snapshot To %d, Update NextIndex To %d, MatchIndex To %d", rf.getServerDetail(), serverNo, idx+1, idx)

	return SNAPSHOTCOMPLETE
}

func (rf *Raft) AcceptSnapshot(args *InstallSnapshot, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]Received Snapshot %v", rf.getServerDetail(), args)
	reply.Term = rf.currentTerm
	if args.Term < rf.getCurrentTerm() {
		return
	}

	if args.Term > rf.getCurrentTerm() || rf.getRole() == CANDIDATE {
		DPrintf("[%v]Received Snapshot From %d, Term:%d > My Term:%d, Turn to follower", rf.getServerDetail(),
			args.Term, rf.getCurrentTerm(), args.LeaderId)
		rf.turnToFollower(args.Term, args.LeaderId)
		rf.persist()
	}

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
	DPrintf("[%v]After Build Snapshot, Log Length:%d, LastLogIdx:%d", rf.getServerDetail(), len(rf.log), rf.getLastLogIndex())
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 只有在提交Command到Chan时，SnapShot才可能被调用
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]Build Snapshot, Index:%d, Sz:%d", rf.getServerDetail(), index, len(snapshot))
	snap := &Snapshot{}

	if rf.snapshot != nil && rf.snapshot.LastIncludedIndex >= int32(index) {
		return
	}

	snap.LastIncludedIndex = int32(index)
	snap.LastIncludedTerm = rf.getLogTermByIdx(int32(index))
	snap.Data = make([]byte, len(snapshot))
	copy(snap.Data, snapshot)

	// 把[0~index]之间的日志都删掉
	pos := rf.getLogPosByIdx(int32(index)) + 1
	rf.log = rf.log[pos:]
	rf.snapshot = snap
	rf.persist()

	DPrintf("[%v]Remov Log Before Index:%d, LastIncludedIndex:%d, LastIncludedTerm:%d, SnapSize:%d",
		rf.getServerDetail(), rf.getLastLogIndex(), rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm, len(rf.snapshot.Data))
	for idx, entry := range rf.log {
		DPrintf("    [%v]Snapshot Idx:%d LogIndex:%d", rf.getServerDetail(), idx, entry.Index)
	}
}
