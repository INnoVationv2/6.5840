package raft

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNotLog = errors.New("ERROR NOT LOG")
)

type InMemoryLog struct {
	serverId                int
	mu                      sync.RWMutex
	log                     []LogEntry
	snapshotLastIncludedIdx int32
}

func buildInMemoryLog(serverId int) *InMemoryLog {
	l := &InMemoryLog{
		serverId: serverId,
	}
	l.log = append(l.log, LogEntry{Index: 0, Term: 0})
	return l
}

func (l *InMemoryLog) setLog(logs []LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = logs
}

func (l *InMemoryLog) getOne(idx int32, log *LogEntry) (err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if pos := l.convertIdxToPos(idx); pos != -1 {
		*log = l.log[pos]
		return
	}
	err = ErrNotLog
	return
}

func (l *InMemoryLog) getRange(st, ed int32) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	stPos, edPos := l.convertIdxToPos(st), l.convertIdxToPos(ed)
	if stPos == -1 || edPos == -1 {
		return nil
	}

	logSlice := l.log[stPos : edPos+1]
	if len(logSlice) == 0 {
		return nil
	}
	logs := make([]LogEntry, len(logSlice))
	copy(logs, logSlice)
	return logs
}

func (l *InMemoryLog) getAll() []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	logs := make([]LogEntry, len(l.log))
	copy(logs, l.log)
	return logs
}

// Delete Log[START~Idx], e.g.Remain Log[Idx+1~END]
func (l *InMemoryLog) deleteBefore(idx int32) {
	l.mu.Lock()
	defer l.mu.Unlock()

	DPrintf("[%d]Delete Before %d", l.serverId, idx)

	if len(l.log) == 0 {
		return
	}

	if idx >= l.log[len(l.log)-1].Index {
		l.log = l.log[:0]
		return
	}

	if pos := l.convertIdxToPos(idx); pos != -1 {
		DPrintf("[%d]Delete Log Before %d, Pos:%d, RealIdx:%d,", l.serverId, idx, pos, l.log[pos].Index)
		l.log = l.log[pos+1:]
		DPrintf("[%d]After Delete, Log Len Is %d", l.serverId, len(l.log))
	}
}

// Delete Log[idx~END], e.g.Remain Log[0~Idx)
// Golang Slice Rule Is [), l.log[:pos] Will Not Remain log[pos]
func (l *InMemoryLog) deleteAfter(idx int32) {
	l.mu.Lock()
	defer l.mu.Unlock()

	DPrintf("[%d]Delete After %d", l.serverId, idx)

	if pos := l.convertIdxToPos(idx); pos != -1 {
		l.log = l.log[:pos]
	}
}

func (l *InMemoryLog) appendOne(entry *LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	DPrintf("[%d]Append One", l.serverId)

	l.log = append(l.log, *entry)

	if Debug {
		for i := 0; i < len(l.log); i++ {
			DPrintf("  [%d]Index:%d, Term:%d", l.serverId, l.log[i].Index, l.log[i].Term)
		}
	}
}

func (l *InMemoryLog) appendSlice(entries []LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	//if n := len(l.log); DevMode && n != 0 {
	//	lastLogIdx := l.lastLog().Index
	//	Assert(entries[0].Index == lastLogIdx+1,
	//		fmt.Sprintf("[%d]NewLogIdx:%d, LastLogIdx:%d, logLen:%d",
	//			l.serverId, entries[0].Index, lastLogIdx, n))
	//}

	DPrintf("[%d]Append Slice", l.serverId)
	l.log = append(l.log, entries...)

	if Debug {
		for i := 0; i < len(l.log); i++ {
			DPrintf("  [%d]Index:%d, Term:%d", l.serverId, l.log[i].Index, l.log[i].Term)
		}
	}
}

func (l *InMemoryLog) lastLog() *LogEntry {
	return &l.log[len(l.log)-1]
}

func (l *InMemoryLog) convertIdxToPos(idx int32) int32 {
	if len(l.log) == 0 ||
		idx < l.log[0].Index ||
		idx > l.log[len(l.log)-1].Index {
		return -1
	}
	pos := idx - l.log[0].Index
	if Debug {
		Assert(l.log[pos].Index == idx,
			fmt.Sprintf("InMemoryLog/convertIdxToPos: "+
				"Idx:%d, pos:%d, realIdx:%d", idx, pos, l.log[pos].Index))
	}
	return pos
}

func (l *InMemoryLog) len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.log)
}

type InMemorySnapshot struct {
	mu                sync.RWMutex
	content           []byte
	lastIncludedIndex int32
	lastIncludedTerm  int32
}

func (s *InMemorySnapshot) available() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s._available()
}

func (s *InMemorySnapshot) _available() bool {
	return s.content != nil && len(s.content) != 0
}

func (s *InMemorySnapshot) getSnapshotInfo() (lastIncludedIndex int32, lastIncludedTerm int32) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s._available() {
		return s.lastIncludedIndex, s.lastIncludedTerm
	}
	return -1, -1
}

func (s *InMemorySnapshot) lastIncludeIndex() int32 {
	index, _ := s.getSnapshotInfo()
	return index
}

func (s *InMemorySnapshot) lastIncludeTerm() int32 {
	_, term := s.getSnapshotInfo()
	return term
}

func (s *InMemorySnapshot) data() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s._available() {
		data := make([]byte, len(s.content))
		copy(data, s.content)
		return data
	}
	return nil
}

func (s *InMemorySnapshot) setSnapshot(lastIdx, lastTerm int32, data []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if lastIdx <= s.lastIncludedIndex {
		return false
	}

	s.lastIncludedIndex, s.lastIncludedTerm = lastIdx, lastTerm
	s.content = data
	return true
}

func (s *InMemorySnapshot) getSnapshot() (lastIdx, lastTerm int32, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastIdx, lastTerm = s.lastIncludedIndex, s.lastIncludedTerm
	data = make([]byte, len(s.content))
	copy(data, s.content)
	return
}
