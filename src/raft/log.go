package raft

import (
	"fmt"
	"time"
)

type LogEntry struct {
	Term    int32
	Command interface{}
}

func (rf *Raft) buildLogEntry(command interface{}, term int32) *LogEntry {
	return &LogEntry{
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
	return fmt.Sprintf("{Term:%d PrevLogIndex:%d PrevLogTerm:%d LeaderCommit:%d Len:%d}",
		args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
}

func (rf *Raft) buildAppendEntriesArgs(args *AppendEntriesArgs, idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args.Term = rf.getCurrentTerm()
	args.LeaderId = rf.me
	args.LeaderCommit = rf.getCommitIndex()

	nextIdx := rf.getNextIndex(idx)
	prevLogIdx := nextIdx - 1
	args.PrevLogIndex = prevLogIdx
	args.PrevLogTerm = rf.log[prevLogIdx].Term
	args.Entries = rf.log[nextIdx:]
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

	DPrintf("[%v]Receive New Command\n", rf.getServerDetail())
	isLeader = true
	index = len(rf.log)
	currentTerm := rf.getCurrentTerm()
	term = int(currentTerm)
	logEntry := rf.buildLogEntry(command, currentTerm)
	rf.log = append(rf.log, *logEntry)

	go rf.syncLogWithFollower()

	return
}

// 发送日志到所有Server，达成一致后对日志提交
func (rf *Raft) syncLogWithFollower() {
	ok := rf.openLogSyncing()
	if !ok {
		DPrintf("[%v]Log Syncing, Exit", rf.getServerDetail())
		return
	}
	DPrintf("[%v]Start Sync Log", rf.getServerDetail())
	defer DPrintf("[%v]End Sync Log", rf.getServerDetail())

	jobChan := make(chan int)
	for idx := range rf.peers {
		if int32(idx) == rf.me {
			continue
		}
		idx := idx
		go func() {
			ok := rf.sendEntriesToFollower(idx)
			if !ok {
				jobChan <- -1
			} else {
				jobChan <- idx
			}
		}()
	}
	//for idx := 0; idx < len(rf.peers); idx++ {
	//	if int32(idx) == rf.me {
	//		continue
	//	}
	//	idx := idx
	//	go func() {
	//		ok := rf.sendEntriesToFollower(idx)
	//		if !ok {
	//			jobChan <- -1
	//			return
	//		}
	//		jobChan <- idx
	//	}()
	//}

	// 超过半数更新成功，或者所有Server都返回结果
	N := int32(-1)
	peerNum, majority := len(rf.peers)-1, rf.majority
	for ; rf.isLeader() && !rf.killed() &&
		majority > 0 && peerNum > 0; peerNum-- {
		select {
		case idx := <-jobChan:
			if idx == -1 {
				continue
			}

			majority--
			if N == -1 {
				N = rf.getMatchIndex(idx)
			}
			N = min(N, rf.getMatchIndex(idx))
		}
	}

	if majority != 0 {
		// 没有达成共识
		DPrintf("[%v]syncLogWithFollower Didn't Aggrement", rf.getServerDetail())
		rf.closeLogSyncing()
		return
	}

	rf.mu.Lock()
	if N > rf.getCommitIndex() &&
		rf.log[N].Term == rf.getCurrentTerm() {
		rf.setCommitIndex(N)
		DPrintf("[%v]Update CommitIndex To %d", rf.getServerDetail(), N)
	}

	commitIndex := rf.getCommitIndex()
	if commitIndex > rf.lastApplied {
		rf.sendCommitedLogToTester(rf.lastApplied+1, commitIndex)
		rf.lastApplied = commitIndex
	}
	rf.mu.Unlock()

	rf.closeLogSyncing()
}

func (rf *Raft) sendEntriesToFollower(idx int) bool {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	for !rf.killed() && rf.getRole() == LEADER {
		rf.buildAppendEntriesArgs(args, idx)
		DPrintf("[%v]Send AppendEntries RPC:%v to follower: %v\n", rf.getServerDetail(), args, idx)
		ok := rf.peers[idx].Call("Raft.AcceptAppendEntries", args, reply)
		if !ok {
			DPrintf("[%v]Send AppendEntries RPC To %d Failed", rf.getServerDetail(), idx)
			return false
		}

		if reply.Success {
			log := fmt.Sprintf("[%v]Success Send AppendEntries To %d", rf.getServerDetail(), idx)
			if len(args.Entries) != 0 {
				index := args.PrevLogIndex + int32(len(args.Entries))
				rf.mu.Lock()
				rf.setNextIndex(idx, index+1)
				rf.setMatchIndex(idx, index)
				rf.mu.Unlock()
				log += fmt.Sprintf(", Update nextIndex To %d", index+1)
			}
			DPrintf(log)
			return true
		}

		// 接下来都是reply.Success = false
		rf.mu.Lock()
		if reply.Term > rf.getCurrentTerm() {
			DPrintf("[%v]Follower Term > Leader Term, Back To Follower\n", rf.getServerDetail())
			rf.turnToFollower(reply.Term)
			rf.mu.Unlock()
			return false
		}

		DPrintf("[%v]AppendEntries RPC Failed, Decrease Next Index And Re-Try\n", rf.getServerDetail())
		rf.addNextIndex(idx, -1)
		rf.mu.Unlock()
	}
	return false
}

// For Follower
func (rf *Raft) AcceptAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v]AppendEntries From %d!", rf.getServerDetail(), args.LeaderId)

	term := rf.getCurrentTerm()
	logSz := int32(len(rf.log))
	reply.Term = term
	reply.Success = false

	if args.Term < term {
		return
	}

	// 每条记录都用来更新Heartbeat
	rf.resetElectionTimer()

	role := rf.getRole()
	if args.Term > term || role == CANDIDATE {
		DPrintf("[%s]AcceptAppendEntries Back To Follwer\n", rf.getRoleStr())
		rf.turnToFollower(args.Term)
		rf.leaderId = args.LeaderId
		rf.setVoteFor(args.LeaderId)
		reply.Term = args.Term
	}

	// 本地没有与prevLogIndex、prevLogTerm匹配的项
	// 返回false
	if args.PrevLogIndex >= logSz ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%v]AppendEntries No Matched Log Entry", rf.getServerDetail())
		return
	}

	// 发来的日志和本地日志冲突(index相同,但term不同)
	// 删除冲突条目及后面所有的条目
	i, j := int(args.PrevLogIndex+1), 0
	for i < int(logSz) && j < len(args.Entries) {
		entry1 := rf.log[i]
		entry2 := args.Entries[j]
		if entry1.Term != entry2.Term {
			break
		}
		i, j = i+1, j+1
	}
	rf.log = rf.log[:i]

	// 将发来的日志项添加到日志中
	if len(args.Entries) != 0 {
		rf.log = append(rf.log, args.Entries...)
		logSz = int32(len(rf.log))
		DPrintf("[%v]Append %d Log, LogSz:%d", rf.getServerDetail(), len(args.Entries), logSz)
	}

	// 更新CommitIndex
	if args.LeaderCommit > rf.getCommitIndex() {
		newCommitIndex := min(args.LeaderCommit, logSz-1)
		rf.setCommitIndex(newCommitIndex)
		DPrintf("[%v]AppendEntries Update CommitIndex To %d", rf.getServerDetail(), newCommitIndex)
	}

	// 更新lastApplied
	commitIndex := rf.getCommitIndex()
	if commitIndex > rf.lastApplied {
		DPrintf("[%v]Apply %d~%d Log To State Machine", rf.getServerDetail(), rf.lastApplied+1, commitIndex)
		rf.sendCommitedLogToTester(rf.lastApplied+1, commitIndex)
		rf.lastApplied = commitIndex
	}

	reply.Success = true
	DPrintf("[%v]AppendEntries Success\n", rf.getServerDetail())
}

func (rf *Raft) sendCommitedLogToTester(st, ed int32) {
	DPrintf("[%v]%d~%d, LogSz:%d", rf.getServerDetail(), st, ed, len(rf.log))
	msg := ApplyMsg{CommandValid: true}
	for ; st <= ed; st++ {
		DPrintf("[%v]Send Log %d To Tester\n", rf.getServerDetail(), st)
		msg.CommandIndex = int(st)
		msg.Command = rf.log[st].Command
		rf.applyCh <- msg
	}
}

func (rf *Raft) sendHeartbeat() {
	gap := time.Duration(100) * time.Millisecond
	// 每隔100ms尝试发送心跳到所有Follower
	for !rf.killed() && rf.isLeader() {
		DPrintf("[%v]Send Heartbeat", rf.getServerDetail())
		go rf.syncLogWithFollower()
		time.Sleep(gap)
	}
}
