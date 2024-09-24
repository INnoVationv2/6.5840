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
	return &LogEntry{
		Index:   rf.getLastLogIndex() + 1,
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
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIndex

	nextIdx := rf.nextIndex[serverNo]
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

	DPrintf("[%v]Build AppendEntry For %d, NextIndex:%d, nextLogPos:%d", rf.getServerDetail(), serverNo, nextIdx, nextLogPos)
}

// Lab测试提交命令的地方，但是和客户端提交command不同
// 这里需要立刻返回，而不是等日志提交后才返回结果
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()

	if !rf.isLeader() || rf.killed() {
		rf.mu.Unlock()
		return -1, -1, false
	}
	currentTerm := rf.currentTerm
	logEntry := rf.buildLogEntry(command, currentTerm)
	rf.log = append(rf.log, *logEntry)
	DPrintf("[%v]Append Log, LastLogIdx:%d", rf.getServerDetail(), logEntry.Index)
	rf.persist()

	rf.mu.Unlock()

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
			for !rf.killed() && rf.getRole() == LEADER {
				DPrintf("[%v]Send Entries To Follower %d", rf.getServerDetail(), serverNo)
				status = rf.sendEntriesToFollower(term, serverNo, false)
				// 如果是发送超时或者snapshot发送完成, 需要重试发送
				if status == TIMEOUT {
					DPrintf("[%v]Send AppendEntries RPC To %d Timeout, ReSending", rf.getServerDetail(), serverNo)
					continue
				} else if status == SNAPSHOTCOMPLETE {
					DPrintf("[%v]Success Send Snapshot To %d", rf.getServerDetail(), serverNo)
					continue
				}
				break
			}
			if status == COMPLETE {
				if atomic.AddInt32(&majority, -1) == 0 {
					DPrintf("[%v]Update CommitIndex", rf.getServerDetail())
					rf.updateCommitIndex(term)
				}
			}
		}()
	}
}

func (rf *Raft) updateCommitIndex(term int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() || rf.killed() || term != rf.currentTerm {
		DPrintf("[%v]Update Commit Index Failed, Server Status Changed!", rf.getServerDetail())
		return
	}

	N := rf.findCommitIndex()
	DPrintf("[%v]New CommitIndex:%d", rf.getServerDetail(), N)
	// 只提交当前任期的日志项
	if N > rf.commitIndex && rf.log[rf.getLogPosByIdx(N)].Term == rf.currentTerm {
		rf.commitIndex = N
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), N)
	}

	if rf.commitIndex > rf.lastApplied {
		go rf.sendCommitedLogToTester()
	}
	DPrintf("[%v]Update Commit Index Complete", rf.getServerDetail())
}

func (rf *Raft) sendEntriesToFollower(term int32, serverNo int, heartbeat bool) int {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	for !rf.killed() {
		rf.mu.Lock()
		DPrintf("[%v]Sync Log Entry With Follower %d", rf.getServerDetail(), serverNo)
		if rf.getRole() != LEADER || term != rf.currentTerm {
			DPrintf("[%v]Stop Sync Log With %d, Role:%v, term:%d, currentTerm:%d",
				rf.getServerDetail(), serverNo, rf.getRole(), term, rf.currentTerm)
			rf.mu.Unlock()
			return ERROR
		}

		if rf.snapshot != nil && rf.snapshot.LastIncludedIndex >= rf.nextIndex[serverNo] {
			installSnapshot := rf.buildInstallSnapshot()
			rf.mu.Unlock()
			return rf.sendSnapshotToFollower(serverNo, installSnapshot)
		}

		rf.buildAppendEntriesArgs(args, serverNo, heartbeat)
		if !heartbeat && len(args.Entries) == 0 {
			DPrintf("[%v]Stop Sync Log With %d, Args.Entries is 0", rf.getServerDetail(), serverNo)
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
		if rf.killed() || rf.getRole() != LEADER || rf.currentTerm != args.Term {
			DPrintf("[%v]killed:%v, Role:%v, Term:%d, CurrentTerm:%d", rf.getServerDetail(), rf.killed(), rf.getRoleStr(), args.Term, rf.currentTerm)
			rf.mu.Unlock()
			return ERROR
		}

		if reply.Success {
			DPrintf("[%v]Success Send %d Log Entry To %d", rf.getServerDetail(), len(args.Entries), serverNo)
			if len(args.Entries) != 0 {
				index := args.PrevLogIndex + int32(len(args.Entries))
				rf.nextIndex[serverNo] = max(rf.nextIndex[serverNo], index+1)
				rf.matchIndex[serverNo] = max(rf.matchIndex[serverNo], index)
				DPrintf("[%v]Update %d nextIndex To %d, matchIndex To %d", rf.getServerDetail(), serverNo, index+1, index)
			}
			rf.mu.Unlock()
			return COMPLETE
		}

		// 接下来都是reply.Success = false
		if reply.Term > rf.currentTerm {
			DPrintf("[%v]Follower:%d Term > My Term, Back To Follower\n", rf.getServerDetail(), serverNo)
			rf.turnToFollower(reply.Term, -1)
			rf.persist()
			rf.mu.Unlock()
			return ERROR
		}

		if reply.XTerm == -1 && reply.XIndex == -1 {
			// Follower日志比Leader短
			rf.nextIndex[serverNo] = min(rf.nextIndex[serverNo], reply.XLen)
		} else {
			pos := max(int32(len(rf.log)-1), 0)
			for pos > 0 && rf.log[pos].Term > reply.XTerm {
				pos--
			}
			if rf.log[pos].Term == reply.XTerm {
				rf.nextIndex[serverNo] = min(rf.nextIndex[serverNo], rf.log[pos].Index)
			} else {
				rf.nextIndex[serverNo] = min(rf.nextIndex[serverNo], reply.XIndex)
			}
		}
		DPrintf("[%v]AppendEntries RPC To %d Failed, Decrease NextIndex To %d And Re-Try\n",
			rf.getServerDetail(), serverNo, rf.nextIndex[serverNo])
		rf.mu.Unlock()
	}
	return ERROR
}

// For Follower
func (rf *Raft) AcceptAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]New AppendEntries: %v", rf.getServerDetail(), args)

	term := rf.currentTerm
	reply.Term = term
	reply.Success = false
	if args.Term < term {
		return
	}

	// 重置选举超时器
	rf.closeElectionTimer()

	if args.Term > term {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		if rf.getRole() != FOLLOWER {
			rf.setRole(FOLLOWER)
			rf.votedFor = args.LeaderId
		}
		rf.persist()
	}

	// args.Term == term
	if rf.getRole() == CANDIDATE {
		rf.setRole(FOLLOWER)
		rf.votedFor = args.LeaderId
		rf.persist()
	}

	// 处理snapshot.LastLogIndex大于prevLogIndex的情况
	if rf.snapshot != nil && rf.snapshot.LastIncludedIndex > args.PrevLogIndex {
		if len(args.Entries) != 0 {
			idx := 0
			for idx < len(args.Entries) && args.Entries[idx].Index <= rf.snapshot.LastIncludedIndex {
				idx++
			}
			args.Entries = args.Entries[idx:]
		}
		args.PrevLogIndex = rf.snapshot.LastIncludedIndex
		args.PrevLogTerm = rf.snapshot.LastIncludedTerm
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

		// 返回属于冲突Term的第一个条目的index
		if len(rf.log) > 0 && args.PrevLogIndex <= lastLogIdx {
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
		for i < int32(len(rf.log)) && j < len(args.Entries) {
			entry1, entry2 := rf.log[i], args.Entries[j]
			if entry1.Term != entry2.Term {
				// 日志发生冲突, 截断, 去掉rf.log[:i]
				rf.log = rf.log[:i]
				break
			}
			i, j = i+1, j+1
		}
		// 截断args.Entries
		args.Entries = args.Entries[j:]
		if len(args.Entries) != 0 {
			rf.log = append(rf.log, args.Entries...)
			rf.persist()
		}
		DPrintf("[%v]Append %d Log, LastLogIndex:%d", rf.getServerDetail(), len(args.Entries), rf.getLastLogIndex())
	}

	// 更新CommitIndex, Follower同样要任期相同才提交
	newCommitIndex := min(args.LeaderCommit, lastLogIdx)
	if newCommitIndex > rf.commitIndex && args.Term == rf.getLogTermByIdx(newCommitIndex) {
		rf.commitIndex = newCommitIndex
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), newCommitIndex)
	}

	if rf.commitIndex > rf.lastApplied {
		go rf.sendCommitedLogToTester()
	}

	reply.Success = true
	DPrintf("[%v]AppendEntries Success\n", rf.getServerDetail())
}

func (rf *Raft) sendCommitedLogToTester() {
	rf.applyChMutex.Lock()
	defer rf.applyChMutex.Unlock()

	rf.mu.Lock()
	// 发送snapshot
	if rf.snapshot != nil && rf.snapshot.LastIncludedIndex > rf.lastApplied {
		DPrintf("[%v]Send Snapshot %d To Testerd", rf.getServerDetail(), rf.snapshot)
		rf.sendSnapshotToTester(rf.snapshot)
		rf.lastApplied = max(rf.lastApplied, rf.snapshot.LastIncludedIndex)
		DPrintf("[%v]Success Send Snapshot To Tester, Update LastApplied To %d", rf.getServerDetail(), rf.lastApplied)
	}
	if rf.lastApplied >= rf.commitIndex {
		rf.mu.Unlock()
		return
	}
	st, ed := rf.getLogPosByIdx(rf.lastApplied+1), rf.getLogPosByIdx(rf.commitIndex)
	commitLogs := rf.log[st : ed+1]
	logs := make([]LogEntry, len(commitLogs))
	copy(logs, commitLogs)
	DPrintf("[%v]LastApplied:%d,pos%d CommitIndex:%d,pos:%d, FirstLogIdx:%d", rf.getServerDetail(), rf.lastApplied, st, rf.commitIndex, ed, rf.log[st].Index)
	rf.lastApplied = max(rf.lastApplied, logs[len(logs)-1].Index)
	rf.mu.Unlock()

	DPrintf("[%v]Send [%d~%d] Log To Tester", rf.getServerDetail(), logs[0].Index, logs[len(logs)-1].Index)
	msg := ApplyMsg{CommandValid: true}
	for _, log := range logs {
		DPrintf("[%v]Send Log %d To Tester", rf.getServerDetail(), log.Index)
		msg.CommandIndex, msg.Command = int(log.Index), log.Command
		rf.applyCh <- msg
	}
	DPrintf("[%v]Success Send %d~%d Log To Tester, Sz:%d, Update LastApplied To %d", rf.getServerDetail(), logs[0].Index, logs[len(logs)-1].Index, len(logs), rf.lastApplied)
}

func (rf *Raft) sendHeartbeat() {
	DPrintf("[%v]Become Leader, Start Send Heartbeat", rf.getServerDetail())
	gap := time.Duration(100) * time.Millisecond
	// 每隔100ms检查，给100ms内没有发送数据的Follower发送心跳
	for !rf.killed() && rf.isLeader() {
		DPrintf("[%v]Sending Heartbeat", rf.getServerDetail())
		for idx := range rf.peers {
			if idx == int(rf.me) {
				continue
			}
			if rf.heartbeatTimeout(idx) {
				go rf.sendEntriesToFollower(rf.currentTerm, idx, true)
			}
		}
		time.Sleep(gap)
	}
	DPrintf("[%v]Stop Sending Heartbeat, Killed:%v IsLeader:%v", rf.getServerDetail(), rf.killed(), rf.isLeader())
}
