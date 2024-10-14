package raft

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	TIMEOUT = iota
	ERROR
	COMPLETE
	SNAPSHOTCOMPLETE
)

type LogEntry struct {
	Index   int32
	Term    int32
	Command interface{}
}

func (entry *LogEntry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", entry.Term, entry.Command)
}

func (rf *Raft) buildLogEntry(command interface{}) *LogEntry {
	return &LogEntry{
		Index:   rf.getLastLogIndex() + 1,
		Term:    rf.getCurrentTerm(),
		Command: command,
	}
}

type AppendEntriesRequest struct {
	Term         int32
	Leader       int32
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int32
}

func (args *AppendEntriesRequest) String() string {
	return fmt.Sprintf("{Term:%d PrevLogIndex:%d PrevLogTerm:%d Leader:%v LeaderCommit:%d Len:%d}",
		args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Leader, args.LeaderCommit, len(args.Entries))
}

type AppendEntriesResponse struct {
	Term    int32
	Success bool
	XTerm   int32
	XIndex  int32
	XLen    int32
}

func (reply *AppendEntriesResponse) String() string {
	return fmt.Sprintf("{Term:%d Success:%v XTerm:%d XIndex:%d XLen:%d}",
		reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
}

type replicationState struct {
	currentTerm int32
	id          int
	leaderId    int32
	nextIndex   int32
	lastContact int64
	stopChan    chan struct{}
	triggerChan chan struct{}

	commitment *commitment
}

func (s *replicationState) getLastContact() int64 {
	return atomic.LoadInt64(&s.lastContact)
}

func (s *replicationState) setLastContact() {
	atomic.StoreInt64(&s.lastContact, now())
}

type commitment struct {
	mu               sync.Mutex
	matchIndex       []int32
	startCommitIndex int32
	commitIndex      int32
	commitCh         chan struct{}
}

func (c *commitment) getCommitIdx() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commitIndex
}

func (c *commitment) matches(idx int, matchIdx int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if matchIdx > c.matchIndex[idx] {
		c.matchIndex[idx] = matchIdx
		c.recalculate()
	}
}

func (c *commitment) recalculate() {
	matched := make([]int32, len(c.matchIndex))
	copy(matched, c.matchIndex)
	sort.Sort(int32Slice(matched))
	DPrintf("%v", matched)
	newCommitIdx := matched[(len(matched)-1)/2]
	if newCommitIdx > c.commitIndex {
		c.commitIndex = newCommitIdx
		asyncNotifyCh(c.commitCh)
	}
}

// Lab测试提交命令的地方，但是和客户端提交command不同
// 这里需要立刻返回，而不是等日志提交后才返回结果
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	//if !rf.isLeader() || rf.killed() {
	//	return -1, -1, false
	//}
	select {
	case <-rf.shutdownCh:
		return -1, -1, false
		//case <-
	}
	rf.logMu.Lock()
	logEntry := rf.buildLogEntry(command)
	rf.log = append(rf.log, *logEntry)
	rf.setMatchIdx(int(rf.me), logEntry.Index)
	rf.logMu.Unlock()
	DPrintf("[%v]Append Log, LastLogIdx:%d", rf.getServerDetail(), logEntry.Index)
	rf.persist()

	for rf.replState == nil {
		time.Sleep(10 * time.Millisecond)
	}
	for _, s := range rf.replState {
		asyncNotifyCh(s.triggerChan)
	}
	return int(logEntry.Index), int(logEntry.Term), true
}

func (rf *Raft) buildAppendEntriesArgs(req *AppendEntriesRequest, s *replicationState, lastIdx int32) {
	req.Leader = s.leaderId
	req.Term = s.currentTerm
	req.LeaderCommit = rf.commitment.getCommitIdx()
	nextLogPos, lastLogPos := rf.getLogPosByIdx(s.nextIndex), rf.getLogPosByIdx(lastIdx)

	if nextLogPos == 0 {
		req.PrevLogIndex, req.PrevLogTerm = rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm
	} else {
		prevLog := rf.log[nextLogPos-1]
		req.PrevLogIndex, req.PrevLogTerm = prevLog.Index, prevLog.Term
	}

	logs := rf.log[nextLogPos : lastLogPos+1]
	req.Entries = make([]LogEntry, len(logs))
	copy(req.Entries, logs)

	DPrintf("[%v]Build AppendEntry For %d, NextIndex:%d, nextLogPos:%d", rf.getServerDetail(), s.id, s.nextIndex, nextLogPos)
}

func (rf *Raft) sendCommitedLogToTester() {
	if rf.snapshot != nil && rf.snapshot.LastIncludedIndex > rf.lastApplied {
		snapshot := &Snapshot{
			LastIncludedIndex: rf.snapshot.LastIncludedIndex,
			LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
			Data:              make([]byte, len(rf.snapshot.Data)),
		}
		copy(snapshot.Data, rf.snapshot.Data)
		rf.lastApplied = max(rf.lastApplied, snapshot.LastIncludedIndex)
		DPrintf("[%v]With Snapshot Update LastApplied To %d", rf.getServerDetail(), rf.lastApplied)
		rf.sendSnapshotToTester(snapshot)
	}

	commitIdx := rf.commitment.getCommitIdx()
	if commitIdx > rf.lastApplied {
		st, ed := rf.getLogPosByIdx(rf.lastApplied+1), rf.getLogPosByIdx(commitIdx)
		rf.logMu.RLock()
		commitLogs := rf.log[st : ed+1]
		logs := make([]LogEntry, len(commitLogs))
		copy(logs, commitLogs)
		rf.logMu.RUnlock()
		rf.lastApplied = max(rf.lastApplied, logs[len(logs)-1].Index)
		rf.sendLogToTester(logs)
		DPrintf("[%v]With LogEntry Update LastApplied To %d", rf.getServerDetail(), rf.lastApplied)
	}
}

func (rf *Raft) sendLogToTester(logs []LogEntry) {
	if len(logs) == 0 {
		return
	}
	DPrintf("[%v]Send [%d~%d] Log To Tester", rf.getServerDetail(), logs[0].Index, logs[len(logs)-1].Index)
	msg := ApplyMsg{CommandValid: true}
	for _, log := range logs {
		DPrintf("[%v]Send Log %d To Tester", rf.getServerDetail(), log.Index)
		msg.CommandIndex, msg.Command = int(log.Index), log.Command
		rf.applyCh <- msg
	}
	DPrintf("[%v]Success Send %d~%d Log To Tester", rf.getServerDetail(), logs[0].Index, logs[len(logs)-1].Index)
}

func (rf *Raft) sendHeartbeat(s *replicationState, stopCh chan struct{}) {
	DPrintf("[%v]Start Send Heartbeat to %d", rf.getServerDetail(), s.id)
	defer DPrintf("[%v]Stop Send Heartbeat to %d", rf.getServerDetail(), s.id)
	req := &AppendEntriesRequest{
		Term:   s.currentTerm,
		Leader: s.leaderId,
	}
	res := &AppendEntriesResponse{}
	gap := time.Millisecond * 100

	for {
		select {
		case <-stopCh:
			return
		case <-time.After(gap):
			if now()-s.getLastContact() >= gap.Milliseconds() {
				req.LeaderCommit = rf.commitment.getCommitIdx()
				DPrintf("[%v]Send Heartbeat to %d", rf.getServerDetail(), s.id)
				rf.sendRPC(s.id, req, res)
			}
		}
	}
}

func (rf *Raft) handleAppendEntriesRPC(req *AppendEntriesRequest, res *AppendEntriesResponse) {
	DPrintf("[%v]Start Handle AppendEntries RPC:%v", rf.getServerDetail(), req)
	term := rf.getCurrentTerm()
	res.Term = term
	res.Success = false
	if req.Term < term {
		return
	}

	rf.updateLastContact()

	if req.Term > term || rf.getRole() == CANDIDATE {
		rf.setRole(FOLLOWER)
		rf.setCurrentTerm(req.Term)
		rf.votedFor = req.Leader
		rf.persist()
		req.Term = rf.getCurrentTerm()
	}

	// 处理snapshot.LastLogIndex大于Request prevLogIndex的情况
	if rf.snapshot != nil && rf.snapshot.LastIncludedIndex > req.PrevLogIndex {
		if len(req.Entries) != 0 {
			idx := 0
			for idx < len(req.Entries) && req.Entries[idx].Index <= rf.snapshot.LastIncludedIndex {
				idx++
			}
			req.Entries = req.Entries[idx:]
		}
		req.PrevLogIndex = rf.snapshot.LastIncludedIndex
		req.PrevLogTerm = rf.snapshot.LastIncludedTerm
	}

	// 没有与prevLogIndex、prevLogTerm匹配的项
	// 返回false
	lastLogIdx := rf.getLastLogIndex()
	pos := rf.getLogPosByIdx(req.PrevLogIndex)
	DPrintf("[%v]PrevLogIndex:%d, pos:%d, LastLogIdx:%d", rf.getServerDetail(), req.PrevLogIndex, pos, lastLogIdx)
	if req.PrevLogIndex > lastLogIdx || rf.getLogTermByIdx(req.PrevLogIndex) != req.PrevLogTerm {
		log := fmt.Sprintf("logSz:%d", lastLogIdx+1)
		res.XTerm = -1
		res.XIndex = -1
		res.XLen = rf.getLogSz()

		// 返回属于冲突Term的第一个条目的index
		if len(rf.log) > 0 && req.PrevLogIndex <= lastLogIdx {
			idx := max(rf.getLogPosByIdx(req.PrevLogIndex), 0)
			res.XTerm = rf.log[idx].Term
			for idx >= 0 && rf.log[idx].Term == res.XTerm {
				idx--
			}
			res.XIndex = rf.log[idx+1].Index
		}
		DPrintf("[%v]AppendEntries %v No Matched Log Entry:%v, %v", rf.getServerDetail(), req, res, log)
		return
	}

	if len(req.Entries) != 0 {
		rf.logMu.Lock()
		i, j := rf.getLogPosByIdx(req.PrevLogIndex)+1, 0
		if i < 0 {
			i = 0
			for j < len(req.Entries) && req.Entries[j].Index <= rf.snapshot.LastIncludedIndex {
				j++
			}
		}
		for i < int32(len(rf.log)) && j < len(req.Entries) {
			entry1, entry2 := rf.log[i], req.Entries[j]
			if entry1.Term != entry2.Term {
				// 日志发生冲突, 截断, 去掉rf.log[:i]
				rf.log = rf.log[:i]
				break
			}
			i, j = i+1, j+1
		}
		// 截断args.Entries
		req.Entries = req.Entries[j:]
		if len(req.Entries) != 0 {
			rf.log = append(rf.log, req.Entries...)
			rf.persist()
		}
		DPrintf("[%v]Append %d Log, LastLogIndex:%d",
			rf.getServerDetail(), len(req.Entries), rf.log[len(rf.log)-1].Index)
		rf.logMu.Unlock()
	}

	// 更新CommitIndex, Follower同样要任期相同才提交
	newCommitIndex := min(req.LeaderCommit, rf.getLastLogIndex())
	if newCommitIndex > rf.commitment.commitIndex && req.Term == rf.getLogTermByIdx(newCommitIndex) {
		rf.commitment.commitIndex = newCommitIndex
		asyncNotifyCh(rf.commitment.commitCh)
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), newCommitIndex)
	}

	res.Success = true
	DPrintf("[%v]AppendEntries Success\n", rf.getServerDetail())
}

func (rf *Raft) logDispatcher(s *replicationState) {
	DPrintf("[%v]Start Log Dispatcher For %d", rf.getServerDetail(), s.id)
	defer DPrintf("[%v]Stop Log Dispatcher For %d", rf.getServerDetail(), s.id)

	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	rf.goFunc(func() { rf.sendHeartbeat(s, stopHeartbeat) }, "sendHeartbeat")

	shouldStop := false
	for !shouldStop {
		select {
		case <-s.stopChan:
			return
		case <-s.triggerChan:
			lastLogIdx := rf.getLastLogIndex()
			shouldStop = rf.sendLogToFollower(s, lastLogIdx)
		}
	}
}

func (rf *Raft) sendLogToFollower(s *replicationState, lastIdx int32) (shouldStop bool) {
	DPrintf("[%v]Sync Log With Follower %d", rf.getServerDetail(), s.id)
	req := &AppendEntriesRequest{}
	res := &AppendEntriesResponse{}
	rf.buildAppendEntriesArgs(req, s, lastIdx)
	if len(req.Entries) == 0 {
		DPrintf("[%v]Stop Sync Log With %d, RPC Entries Size Is 0", rf.getServerDetail(), s.id)
	}

	s.setLastContact()

	DPrintf("[%v]Send AppendEntries RPC:%v To Follower:%v\n", rf.getServerDetail(), req, s.id)
	ok := rf.sendRPC(s.id, req, res)
	if !ok {
		DPrintf("[%v]Send AppendEntries RPC To %d Timeout", rf.getServerDetail(), s.id)
		return
	}

	if res.Success {
		DPrintf("[%v]Success Send %d Log Entry To %d", rf.getServerDetail(), len(req.Entries), s.id)
		if len(req.Entries) != 0 {
			index := req.PrevLogIndex + int32(len(req.Entries))
			s.nextIndex = index + 1
			s.commitment.matches(s.id, index)
			DPrintf("[%v]Update %d nextIndex To %d, matchIndex To %d, CommitIndex To %d",
				rf.getServerDetail(), s.id, s.nextIndex, rf.commitment.matchIndex[s.id], rf.commitment.commitIndex)
		}
		return
	}

	// 接下来都是reply.Success = false
	if res.Term > rf.getCurrentTerm() {
		DPrintf("[%v]Follower:%d Term > My Term, Back To Follower\n", rf.getServerDetail(), s.id)
		rf.turnToFollower(res.Term, -1)
		rf.persist()
		return true
	}

	if res.XTerm == -1 && res.XIndex == -1 {
		// Follower日志比Leader短
		s.nextIndex = res.XLen
	} else {
		pos := max(int32(len(rf.log)-1), 0)
		for pos > 0 && rf.log[pos].Term > res.XTerm {
			pos--
		}
		if rf.log[pos].Term == res.XTerm {
			s.nextIndex = rf.log[pos].Index
		} else {
			s.nextIndex = res.XIndex
		}
	}
	DPrintf("[%v]AppendEntries RPC To %d Failed, Decrease NextIndex To %d And Re-Try\n",
		rf.getServerDetail(), s.id, s.nextIndex)
	return
}
