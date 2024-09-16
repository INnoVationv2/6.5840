package raft

import (
	"fmt"
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

func (rf *Raft) buildLogEntry(command interface{}, term int32) *LogEntry {
	index := rf.getLastLogIndex()
	return &LogEntry{
		Index:   index + 1,
		Term:    term,
		Command: command,
	}
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int32
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int32
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%d PrevLogIndex:%d PrevLogTerm:%d LeaderId:%v LeaderCommit:%d Len:%d}",
		args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderId, args.LeaderCommit, len(args.Entries))
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
	XTerm   int32
	XIndex  int32
	XLen    int32
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%d Success:%v XTerm:%d XIndex:%d XLen:%d}",
		reply.Term, reply.Success, reply.XTerm, reply.XIndex, reply.XLen)
}

func (rf *Raft) buildAppendEntriesArgs(args *AppendEntriesArgs, serverNo int, heartbeat bool) {
	args.LeaderId = rf.me
	args.Term = rf.getCurrentTerm()
	args.LeaderCommit = rf.getCommitIndex()

	nextIdx := rf.getNextIndex(serverNo)
	nextLogPos := rf.getLogPosByIdx(nextIdx)

	if nextLogPos == 0 {
		args.PrevLogIndex, args.PrevLogTerm = rf.snapshot.LastIncludedIndex, rf.snapshot.LastIncludedTerm
	} else {
		prevLog := rf.log[nextLogPos-1]
		args.PrevLogIndex, args.PrevLogTerm = prevLog.Index, prevLog.Term
	}

	if !heartbeat {
		logs := rf.log[nextLogPos:]
		args.Entries = make([]LogEntry, len(logs))
		copy(args.Entries, logs)
	}

	DPrintf("[%v]Build Append Entry For %d, NextIndex:%d, nextLogPos:%d",
		rf.getServerDetail(), serverNo, nextIdx, nextLogPos)
}

// Lab测试提交命令的地方，但是和客户端提交command不同
// 这里需要立刻返回，而不是等日志提交后才返回结果
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() || rf.killed() {
		isLeader = false
		return
	}

	currentTerm := rf.getCurrentTerm()
	logEntry := rf.buildLogEntry(command, currentTerm)
	rf.log = append(rf.log, *logEntry)
	rf.persist()
	DPrintf("[%v]Append Log, LastLogIndex:%d", rf.getServerDetail(), logEntry.Index)

	go rf.syncLogWithFollower(currentTerm)

	return int(logEntry.Index), int(currentTerm), true
}

// 发送日志到所有Server，达成一致后对日志提交
func (rf *Raft) syncLogWithFollower(term int32) {
	DPrintf("[%v]Start Sync Log", rf.getServerDetail())

	majority := rf.majority
	for idx := range rf.peers {
		if int32(idx) == rf.me {
			continue
		}
		serverNo := idx
		go func() {
			var status int
			for !rf.killed() && rf.isLeader() && term == rf.getCurrentTerm() {
				status = rf.sendEntriesToFollower(serverNo, false)
				// 如果是超时发送失败, 需要持续重试
				if status == TIMEOUT || status == SNAPSHOTCOMPLETE {
					DPrintf("[%v]Send AppendEntries RPC To %d Timeout, ReSending", rf.getServerDetail(), serverNo)
					continue
				}
				break
			}
			DPrintf("[%v]Success Send AppendEntries To %d, Status:%v, Killed:%v, isLeader:%v, term:%d", rf.getServerDetail(), serverNo, status, rf.killed(), rf.isLeader(), term)
			if status == COMPLETE {
				if atomic.AddInt32(&majority, -1) == 0 {
					DPrintf("[%v]Update CommitIndex", rf.getServerDetail())
					rf.updateCommitIndex(term)
				}
			}
		}()
	}
	DPrintf("[%v]syncLogWithFollower Complete", rf.getServerDetail())
}

func (rf *Raft) updateCommitIndex(term int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() || rf.killed() || term != rf.getCurrentTerm() {
		DPrintf("[%v]Update Commit Index Fail", rf.getServerDetail())
		return
	}

	N := rf.findCommitIndex()
	// 只提交当前任期的日志项
	if N > rf.getCommitIndex() && rf.log[rf.getLogPosByIdx(N)].Term == rf.getCurrentTerm() {
		rf.setCommitIndex(N)
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), N)
	}

	if rf.getCommitIndex() > rf.lastApplied {
		go rf.sendCommitedLogToTester()
	}
	DPrintf("[%v]Update Commit Index Complete", rf.getServerDetail())
}

func (rf *Raft) sendEntriesToFollower(serverNo int, heartbeat bool) int {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	for !rf.killed() {
		rf.mu.Lock()
		if rf.getRole() != LEADER {
			rf.mu.Unlock()
			return ERROR
		}

		if rf.snapshot != nil && rf.snapshot.LastIncludedIndex >= rf.getNextIndex(serverNo) {
			installSnapshot := rf.buildInstallSnapshot()
			rf.mu.Unlock()
			return rf.sendSnapshotToFollower(serverNo, installSnapshot)
		}

		rf.buildAppendEntriesArgs(args, serverNo, heartbeat)
		if !heartbeat && len(args.Entries) == 0 {
			rf.mu.Unlock()
			return ERROR
		}
		rf.resetHeartbeatTimer(serverNo)
		rf.mu.Unlock()

		DPrintf("[%v]Heartbeat:%v Send AppendEntries RPC:%v to follower: %v\n", rf.getServerDetail(), heartbeat, args, serverNo)
		ok := rf.peers[serverNo].Call("Raft.AcceptAppendEntries", args, reply)
		if !ok {
			DPrintf("[%v]Send AppendEntries RPC To %d Timeout", rf.getServerDetail(), serverNo)
			return TIMEOUT
		}

		rf.mu.Lock()
		if rf.killed() || rf.getRole() != LEADER || rf.getCurrentTerm() != args.Term {
			DPrintf("[%v]killed:%v, Role:%v, Term:%d, CurrentTerm:%d", rf.getServerDetail(), rf.killed(), rf.getRoleStr(), args.Term, rf.getCurrentTerm())
			rf.mu.Unlock()
			return ERROR
		}

		if reply.Success {
			DPrintf("[%v]Success Send %d Log Entry To %d", rf.getServerDetail(), len(args.Entries), serverNo)
			if len(args.Entries) != 0 {
				index := args.PrevLogIndex + int32(len(args.Entries))
				rf.setNextIndex(serverNo, max(rf.getNextIndex(serverNo), index+1))
				rf.setMatchIndex(serverNo, max(rf.getMatchIndex(serverNo), index))
				DPrintf("[%v]Update %d nextIndex To %d, matchIndex To %d", rf.getServerDetail(), serverNo, index+1, index)
			}
			rf.mu.Unlock()
			return COMPLETE
		}
		// 接下来都是reply.Success = false

		if reply.Term > rf.getCurrentTerm() {
			DPrintf("[%v]Follower Term > My Term, Back To Follower\n", rf.getServerDetail())
			rf.turnToFollower(reply.Term, -1)
			rf.persist()
			rf.mu.Unlock()
			return ERROR
		}

		if reply.XTerm == -1 && reply.XIndex == -1 {
			// Follower日志比Leader短
			rf.setNextIndex(serverNo, reply.XLen)
		} else {
			logIdx := max(int32(len(rf.log)-1), 1)
			pos := rf.getLogPosByIdx(logIdx)
			for pos > 1 && rf.log[pos].Term > reply.XTerm {
				pos--
			}
			if rf.log[pos].Term == reply.XTerm {
				rf.setNextIndex(serverNo, rf.log[pos].Index)
			} else {
				rf.setNextIndex(serverNo, reply.XIndex)
			}
		}
		DPrintf("[%v]AppendEntries RPC To %d Failed, Decrease NextIndex To %d And Re-Try\n", rf.getServerDetail(), serverNo, rf.getNextIndex(serverNo))
		rf.mu.Unlock()
	}
	return ERROR
}

// For Follower
func (rf *Raft) AcceptAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]New AppendEntries: %v", rf.getServerDetail(), args)

	term := rf.getCurrentTerm()
	reply.Term = term
	reply.Success = false
	if args.Term < term {
		return
	}

	// 重置选举超时器
	rf.closeElectionTimer()

	if args.Term > term {
		rf.setCurrentTerm(args.Term)
		reply.Term = rf.getCurrentTerm()
		if rf.getRole() != FOLLOWER {
			rf.setRole(FOLLOWER)
			rf.setVoteFor(args.LeaderId)
		}
		rf.persist()
	}
	// args.Term == term
	if rf.getRole() == CANDIDATE {
		rf.setRole(FOLLOWER)
		rf.setVoteFor(args.LeaderId)
		rf.persist()
	}

	// 没有与prevLogIndex、prevLogTerm匹配的项
	// 返回false
	lastLogIdx := rf.getLastLogIndex()
	pos := rf.getLogPosByIdx(args.PrevLogIndex)
	DPrintf("[%v]PrevLogIndex:%d, pos:%d, LastLogIdx:%d", rf.getServerDetail(), args.PrevLogIndex, pos, lastLogIdx)
	if args.PrevLogIndex > lastLogIdx || rf.getLogTermByIdx(args.PrevLogIndex) != args.PrevLogTerm {
		log := fmt.Sprintf("logSz:%d", lastLogIdx+1)
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.getLogSz()

		// 返回属于冲突term的第一个条目的index
		if args.PrevLogIndex <= lastLogIdx {
			idx := max(rf.getLogPosByIdx(args.PrevLogIndex), 0)
			reply.XTerm = rf.log[idx].Term
			for idx >= 0 && rf.log[idx].Term == reply.XTerm {
				idx--
			}
			reply.XIndex = rf.log[idx+1].Index
		}
		DPrintf("[%v]AppendEntries %v No Matched Log Entry:%v, %v", rf.getServerDetail(), args, reply, log)
		return
	}

	if len(args.Entries) != 0 {
		i, j := rf.getLogPosByIdx(args.PrevLogIndex)+1, 0
		if i < 0 {
			i = 0
			for j < len(args.Entries) && args.Entries[j].Index <= rf.snapshot.LastIncludedIndex {
				j++
			}
		}
		DPrintf("[%v]1-i:%d, j:%d", rf.getServerDetail(), i, j)
		for i < int32(len(rf.log)) && j < len(args.Entries) {
			entry1, entry2 := rf.log[i], args.Entries[j]
			if entry1.Term != entry2.Term {
				// 日志发生冲突, 截断, 去掉rf.log[i]
				rf.log = rf.log[:i]
				break
			}
			i, j = i+1, j+1
		}
		DPrintf("[%v]2-i:%d, j:%d", rf.getServerDetail(), i, j)
		// 截断args.Entries
		args.Entries = args.Entries[j:]
		if len(args.Entries) != 0 {
			rf.log = append(rf.log, args.Entries...)
			rf.persist()
		}
		DPrintf("[%v]Append %d Log, LastLogIndex:%d", rf.getServerDetail(), len(args.Entries), rf.getLastLogIndex())
		for idx, entry := range rf.log {
			DPrintf("    [%v]Append Idx:%d LogIndex:%d", rf.getServerDetail(), idx, entry.Index)
		}
	}

	// 更新CommitIndex, Follower同样要任期相同才提交
	newCommitIndex := min(args.LeaderCommit, lastLogIdx)
	logTerm := rf.getLogTermByIdx(newCommitIndex)
	if newCommitIndex > rf.getCommitIndex() && args.Term == logTerm {
		rf.setCommitIndex(newCommitIndex)
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), newCommitIndex)
		go rf.sendCommitedLogToTester()
	}

	reply.Success = true
	DPrintf("[%v]AppendEntries Success\n", rf.getServerDetail())
}

func (rf *Raft) sendCommitedLogToTester() {
	rf.applyChMutex.Lock()
	defer rf.applyChMutex.Unlock()
	DPrintf("[%v]Start Send Log To Tester:[%d~%d]", rf.getServerDetail(), rf.lastApplied+1, rf.getCommitIndex())

	if rf.snapshot != nil && rf.lastApplied < rf.snapshot.LastIncludedIndex {
		DPrintf("[%v]Send Snapshot To Tester, [%d~%d]", rf.getServerDetail(), rf.lastApplied, rf.snapshot.LastIncludedIndex)
		msg := ApplyMsg{
			SnapshotValid: true,
			SnapshotIndex: int(rf.snapshot.LastIncludedIndex),
			SnapshotTerm:  int(rf.snapshot.LastIncludedTerm),
			Snapshot:      rf.snapshot.Data,
		}
		DPrintf("[%v]Snapshot Orgin Size:%d, Msg Size:%d", rf.getServerDetail(), len(rf.snapshot.Data), len(msg.Snapshot))
		rf.applyCh <- msg
		rf.lastApplied = int32(msg.SnapshotIndex)
		DPrintf("[%v]Send Snapshot%v To Tester", rf.getServerDetail(), rf.snapshot)
	}

	rf.mu.Lock()
	if rf.lastApplied >= rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	st, ed := rf.getLogPosByIdx(rf.lastApplied+1), rf.getLogPosByIdx(rf.commitIndex)
	subLog := rf.log[st : ed+1]
	logs := make([]LogEntry, len(subLog))
	copy(logs, subLog)
	DPrintf("[%v]LastApplied:%d,pos%d CommitIndex:%d,pos:%d, FirstLogIdx:%d", rf.getServerDetail(), rf.lastApplied, st, rf.commitIndex, ed, rf.log[st].Index)
	rf.mu.Unlock()

	msg := ApplyMsg{CommandValid: true}
	for _, log := range logs {
		msg.CommandIndex, msg.Command = int(log.Index), log.Command
		DPrintf("[%v]Send Log %d To Tester", rf.getServerDetail(), log.Index)
		rf.applyCh <- msg
	}
	rf.lastApplied = logs[len(logs)-1].Index

	DPrintf("[%v]Send %d~%d Log To Tester, Sz:%d", rf.getServerDetail(), logs[0].Index, logs[len(logs)-1].Index, len(logs))
	DPrintf("[%v]Send Log To Tester Complete, Update LastApplied To %d", rf.getServerDetail(), rf.lastApplied)
}

func (rf *Raft) sendHeartbeat() {
	DPrintf("[%v]Become Leader, Start Send Heartbeat", rf.getServerDetail())
	times := 0
	gap := time.Duration(100) * time.Millisecond
	// 每隔100ms检查，给100ms内没有发送数据的Follower发送心跳
	for !rf.killed() && rf.isLeader() {
		times++
		DPrintf("[%v]%dth Sending Heartbeat", rf.getServerDetail(), times)
		for idx := range rf.peers {
			if idx == int(rf.me) {
				continue
			}
			if rf.heartbeatTimeout(idx) {
				DPrintf("[%v]Send Heartbeat to %d", rf.getServerDetail(), idx)
				go rf.sendEntriesToFollower(idx, true)
			}
		}
		time.Sleep(gap)
	}
	DPrintf("[%v]Stop Sending Heartbeat, Killed:%v IsLeader:%v", rf.getServerDetail(), rf.killed(), rf.isLeader())
}
