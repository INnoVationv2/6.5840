package raft

type leaderState struct {
	id          int
	currentTerm int32
	matchIndex  []int32
	triggerChan chan struct{}
}

//	snapshot := rf.getSnapshot()
//	if snapshot != nil && snapshot.LastIncludedIndex >= s.nextIndex {
//		goto SEND_SNAP
//	}
//
//SEND_SNAP:
//	rf.sendSnapshot(s)
//	installSnapshot := &InstallSnapshotRequest{
//		Term:              s.currentTerm,
//		Leader:          rf.me,
//		LastIncludedIndex: rf.snapshot.LastIncludedIndex,
//		LastIncludedTerm:  rf.snapshot.LastIncludedTerm,
//		Data:              make([]byte, len(rf.snapshot.Data)),
//	}
//	copy(snapshot.Data, rf.snapshot.Data)
//	return installSnapshot
//	rf.mu.Unlock()
//	status := rf.sendSnapshotToFollower(s.id, installSnapshot)
//	if status == ERROR {
//		return true
//	}
//
//	req := &AppendEntriesRequest{}
//	res := &AppendEntriesResponse{}
//	for {
//		rf.mu.Lock()
//		if rf.killed() || !rf.isLeader() {
//			rf.mu.Unlock()
//			return ERROR
//		}
//		DPrintf("[%v]Sync Log With Follower %d", rf.getServerDetail(), serverNo)
//		if rf.snapshot != nil && rf.snapshot.LastIncludedIndex >= rf.nextIndex[serverNo] {
//			installSnapshot := rf.buildInstallSnapshot()
//			rf.mu.Unlock()
//			status := rf.sendSnapshotToFollower(serverNo, installSnapshot)
//			if status == ERROR {
//				return ERROR
//			}
//			continue
//		}
//
//		rf.buildAppendEntriesArgs(req, serverNo, heartbeat)
//		if !heartbeat && len(req.Entries) == 0 {
//			DPrintf("[%v]Stop Sync Log With %d, RPC Entries Size Is 0", rf.getServerDetail(), serverNo)
//			rf.mu.Unlock()
//			return COMPLETE
//		}
//		rf.resetHeartbeatTimer(serverNo)
//		rf.mu.Unlock()
//
//		DPrintf("[%v]Heartbeat:%v Send AppendEntries RPC:%v To Follower:%v\n", rf.getServerDetail(), heartbeat, req, serverNo)
//		ok := rf.peers[serverNo].Call("Raft.AcceptAppendEntries", req, res)
//		if !ok {
//			if rf.checkRaftStatus(req.Term) {
//				return ERROR
//			}
//			DPrintf("[%v]Send AppendEntries RPC To %d Timeout", rf.getServerDetail(), serverNo)
//			time.Sleep(time.Millisecond * 100)
//			continue
//		}
//
//		rf.mu.Lock()
//		if rf.checkRaftStatus(req.Term) {
//			DPrintf("[%v]killed:%v, Role:%v, Term:%d, CurrentTerm:%d", rf.getServerDetail(), rf.killed(), rf.getRoleStr(), req.Term, rf.getCurrentTerm())
//			rf.mu.Unlock()
//			return ERROR
//		}
//
//		if res.Success {
//			DPrintf("[%v]Success Send %d Log Entry To %d", rf.getServerDetail(), len(req.Entries), serverNo)
//			if len(req.Entries) != 0 {
//				index := req.PrevLogIndex + int32(len(req.Entries))
//				rf.nextIndex[serverNo] = max(rf.nextIndex[serverNo], index+1)
//				rf.matchIndex[serverNo] = max(rf.matchIndex[serverNo], index)
//				DPrintf("[%v]Update %d nextIndex To %d, matchIndex To %d", rf.getServerDetail(), serverNo, index+1, index)
//			}
//			rf.mu.Unlock()
//			return COMPLETE
//		}
//
//		// 接下来都是reply.Success = false
//		if res.Term > rf.getCurrentTerm() {
//			DPrintf("[%v]Follower:%d Term > My Term, Back To Follower\n", rf.getServerDetail(), serverNo)
//			rf.turnToFollower(res.Term, -1)
//			rf.persist()
//			rf.mu.Unlock()
//			return ERROR
//		}
//
//		if res.XTerm == -1 && res.XIndex == -1 {
//			// Follower日志比Leader短
//			rf.nextIndex[serverNo] = min(rf.nextIndex[serverNo], res.XLen)
//		} else {
//			pos := max(int32(len(rf.log)-1), 0)
//			for pos > 0 && rf.log[pos].Term > res.XTerm {
//				pos--
//			}
//			if rf.log[pos].Term == res.XTerm {
//				rf.nextIndex[serverNo] = min(rf.nextIndex[serverNo], rf.log[pos].Index)
//			} else {
//				rf.nextIndex[serverNo] = min(rf.nextIndex[serverNo], res.XIndex)
//			}
//		}
//		DPrintf("[%v]AppendEntries RPC To %d Failed, Decrease NextIndex To %d And Re-Try\n",
//			rf.getServerDetail(), serverNo, rf.nextIndex[serverNo])
//		rf.mu.Unlock()
//	}
//}

type RPC struct {
	req       interface{}
	res       interface{}
	replyChan chan struct{}
}
