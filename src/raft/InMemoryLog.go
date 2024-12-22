package raft

import (
	"sync"
)

type InMemoryLog struct {
	mu  sync.RWMutex
	log []LogEntry
}

func buildInMemoryLog() *InMemoryLog {
	l := &InMemoryLog{}
	l.log = append(l.log, LogEntry{Index: 0, Term: 0})
	return l
}

func (l *InMemoryLog) setLog(logs []LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = logs
}

func (l *InMemoryLog) getOne(idx int32) *LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	pos := l.convertIdxToPos(idx)
	if pos < 0 || pos >= int32(len(l.log)) {
		return nil
	}
	return &l.log[pos]
}

func (l *InMemoryLog) getLast() *LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.log) == 0 {
		return nil
	}
	return &l.log[len(l.log)-1]
}

func (l *InMemoryLog) getRange(st, ed int32) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	stPos, edPos := l.convertIdxToPos(st), l.convertIdxToPos(ed)
	logSlice := l.log[stPos : edPos+1]

	DPrintf("Get Range, st:%d, stPos:%d, "+
		"ed:%d, edPos:%d", st, stPos, ed, edPos)

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

// Delete Log Before Idx, e.g.Remain Log After Idx
// Will Retain log[idx]
func (l *InMemoryLog) deleteBefore(idx int32) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.log = l.log[idx:]
}

// Delete Log After Idx(Include Log[Idx])
// e.g.Remain Log Before Idx
func (l *InMemoryLog) deleteAfter(idx int32) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.log = l.log[:idx]
}

func (l *InMemoryLog) appendOne(log *LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.log = append(l.log, *log)
}

func (l *InMemoryLog) appendSlice(logs []LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.log = append(l.log, logs...)
}

func (l *InMemoryLog) convertIdxToPos(idx int32) int32 {
	Assert(len(l.log) != 0, "Log Shouldn't Be Empty")
	return idx - l.log[0].Index
}

func (l *InMemoryLog) getLen() int {
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
	return s.content != nil && len(s.content) != 0
}

func (s *InMemorySnapshot) lastIncludeIndex() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.available() {
		return s.lastIncludedIndex
	}
	return -1
}

func (s *InMemorySnapshot) lastIncludeTerm() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.available() {
		return s.lastIncludedTerm
	}
	return -1
}

func (s *InMemorySnapshot) data() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.available() {
		data := make([]byte, len(s.content))
		copy(data, s.content)
		return data
	}
	return nil
}

func (s *InMemorySnapshot) setSnapshot(lastIdx, LastTerm int32, content []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastIncludedIndex, s.lastIncludedTerm = lastIdx, LastTerm
	s.content = content
}
