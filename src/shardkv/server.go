package shardkv

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	GET = iota
	PUT
	APPEND
)

type Command struct {
	ClientId int64
	CmdId    int32

	Type   int
	Key    string
	Value  string
	Status Err
}

func (cmd *Command) String() string {
	return fmt.Sprintf("{%s %s->%s}", cmdTypeToStr(cmd.Type), cmd.Key, cmd.Value)
}

func buildCommand(opType int, clientId int64, cmdId int32, str ...string) *Command {
	cmd := Command{ClientId: clientId, CmdId: cmdId, Type: opType, Key: str[0], Status: OK}
	if opType == PUT || opType == APPEND {
		cmd.Value = str[1]
	}
	return &cmd
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big

	ctrlers          []*labrpc.ClientEnd
	shardCtrlerClerk *shardctrler.Clerk
	shardConf        shardctrler.Config

	raftPersister *raft.Persister

	db map[string]string

	dead   int32
	killCh chan bool

	raftTerm int32
	// Client前一个提交的Command
	prevCmd map[int64]*Command
	// Client已提交的Command
	submitCmd map[int]*Command

	// 用于记录已经执行的最大的LogEntry的Index
	appliedLogIdx int32
	// Client已执行过的最大Command编号
	matchIndex map[int64]int32
	history    map[int64]map[int32]string
}

func (kv *ShardKV) checkIfCommandAlreadyExecuted(clientId int64, commandId int32) bool {
	if kv.matchIndex[clientId] >= commandId {
		return true
	}
	return false
}

func (kv *ShardKV) updateShardConf() {
	kv.shardConf = kv.shardCtrlerClerk.Query(-1)
}

func (kv *ShardKV) checkShard(key string) bool {
	kv.updateShardConf()
	shard := key2shard(key)
	if kv.shardConf.Shards[shard] == kv.gid {
		return true
	}
	return false
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[%v]Received Get RPC:%v", kv.getServerDetail(), args)
	clientId, cmdId := args.ClientId, args.CommandId

	// 如果是重复命令，直接返回之前的结果
	kv.mu.Lock()
	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.checkIfCommandAlreadyExecuted(clientId, cmdId) {
		DPrintf("[%s]Dupliacte Get Reuqest %v", kv.getServerDetail(), args)
		reply.Err = OK
		reply.Value = kv.history[clientId][cmdId]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	cmd := buildCommand(GET, clientId, cmdId, args.Key)
	kv.submitCommand(cmd)
	reply.Err = cmd.Status
	if reply.Err != OK {
		DPrintf("[%s]Submit Command %v Failed:%v", kv.getServerDetail(), args, reply.Err)
		return
	}
	reply.Value = cmd.Value
	DPrintf("[%v]Get Complete Key:%s Result:%s", kv.getServerDetail(), cmd.Key, cmd.Value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[%v]Received %s RPC:%v", kv.getServerDetail(), args.Op, args)
	clientId, cmdId := args.ClientId, args.CommandId
	kv.mu.Lock()
	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.checkIfCommandAlreadyExecuted(clientId, cmdId) {
		DPrintf("[%s]Dupliacte %s Reuqest %v", kv.getServerDetail(), args.Op, args)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opType int
	switch args.Op {
	case "Put":
		opType = PUT
	case "Append":
		opType = APPEND
	}
	cmd := buildCommand(opType, clientId, cmdId, args.Key, args.Value)
	kv.submitCommand(cmd)
	reply.Err = cmd.Status
	if reply.Err != OK {
		DPrintf("[%v]Command %v failed:%v", kv.getServerDetail(), cmd, reply.Err)
		return
	}
}

func (kv *ShardKV) submitCommand(cmd *Command) {
	DPrintf("[%v]SubmitCommand", kv.getServerDetail())
	kv.mu.Lock()
	cmdIdx, term, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		cmd.Status = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.setRaftTerm(int32(term))
	kv.submitCmd[cmdIdx] = cmd
	kv.mu.Unlock()

	DPrintf("[%s]Submit Command %v To Raft, Index:%d, Term:%d", kv.getServerDetail(), cmd, cmdIdx, term)
	for !kv.killed() && kv.getAppliedLogIdx() < int32(cmdIdx) && kv.getRaftTerm() == int32(term) {
	}

	if cmd.Status == LogNotMatch {
		DPrintf("[%s]Command %v Not Match, Need Re-Submit To KVServer", kv.getServerDetail(), cmd)
	}

	if kv.getRaftTerm() != int32(term) {
		cmd.Status = TermChanged
		DPrintf("[%s]Command %v Is Expired, SubmitTerm:%d, CurrentTerm:%d, Need Re-Submit To KVServer",
			kv.getServerDetail(), cmd, term, kv.getRaftTerm())
	}

	if kv.killed() {
		cmd.Status = Killed
	}
}

func (kv *ShardKV) ticker() {
	DPrintf("[%s]Ticker Start", kv.getServerDetail())
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if msg.SnapshotValid {
				DPrintf("[%s]Receive Snapshot From Raft applyChan, LastIncludeIdx:%d", kv.getServerDetail(), msg.SnapshotIndex)
				kv.readSnapshot(msg.SnapshotIndex, msg.Snapshot)
			}

			if msg.CommandValid {
				cmdIdx, cmd := msg.CommandIndex, msg.Command.(Command)
				DPrintf("[%s]Receive %s Command %d:%v From Raft applyChan", kv.getServerDetail(), cmdTypeToStr(cmd.Type), cmdIdx, &cmd)
				kv.applyCommand(cmdIdx, &cmd)
				kv.setAppliedLogIdx(int32(cmdIdx))
				kv.prevCmd[cmd.ClientId] = &cmd
				kv.checkSnapshot()
			}
			kv.mu.Unlock()
		case <-kv.killCh:
			DPrintf("[%s]Sever Been Killed, Ticker End", kv.getServerDetail())
			return
		}
	}
}

func (kv *ShardKV) applyCommand(cmdIdx int, cmd *Command) {
	prevCmd, ok := kv.prevCmd[cmd.ClientId]
	if !ok || !compareCommand(cmd, prevCmd) {
		switch cmd.Type {
		case PUT:
			kv.db[cmd.Key] = cmd.Value
		case APPEND:
			val, ok := kv.db[cmd.Key]
			if !ok {
				val = ""
			}
			kv.db[cmd.Key] = val + cmd.Value
		case GET:
		}
	}

	cmd2, ok := kv.submitCmd[cmdIdx]
	if !ok {
		return
	}

	if !compareCommand(cmd, cmd2) {
		DPrintf("[%v]Log At %d Not Match, Return", kv.getServerDetail(), cmdIdx)
		cmd2.Status = LogNotMatch
		return
	}
	kv.updateState(cmd2)
	delete(kv.submitCmd, cmdIdx)
}

func (kv *ShardKV) updateState(cmd *Command) {
	clientId, cmdId := cmd.ClientId, cmd.CmdId

	cmd.Status = OK
	kv.matchIndex[clientId] = max(kv.matchIndex[clientId], cmdId)
	DPrintf("[%v]Update Match Index To %d", kv.getServerDetail(), kv.matchIndex[clientId])

	if cmd.Type == GET {
		val, ok := kv.db[cmd.Key]
		if !ok {
			val = ""
		}
		cmd.Value = val
		if kv.history[clientId] == nil {
			kv.history[clientId] = make(map[int32]string)
		}
		kv.history[clientId][cmdId] = val
	}
}

func (kv *ShardKV) readSnapshot(lastIncludeIndex int, snapshot []byte) {
	DPrintf("[%v]lastIncludeIndex:%d AppliedLogIdx:%d", kv.getServerDetail(), lastIncludeIndex, kv.appliedLogIdx)
	if int32(lastIncludeIndex) <= kv.getAppliedLogIdx() {
		return
	}

	kv.setAppliedLogIdx(int32(lastIncludeIndex))
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.db) != nil ||
		d.Decode(&kv.prevCmd) != nil ||
		d.Decode(&kv.submitCmd) != nil ||
		d.Decode(&kv.history) != nil ||
		d.Decode(&kv.matchIndex) != nil {
		log.Fatalf("[%v]Decode Raft State Failed", kv.getServerDetail())
	}
}

func (kv *ShardKV) checkSnapshot() {
	if kv.maxraftstate == -1 {
		return
	}
	DPrintf("[%v]Maxraftstate:%d RaftStateSize:%d", kv.getServerDetail(), kv.maxraftstate, kv.raftPersister.RaftStateSize())
	if kv.maxraftstate-kv.raftPersister.RaftStateSize() > 50 {
		return
	}
	DPrintf("[%v]Build Snapshot", kv.getServerDetail())
	// 大小接近，进行snapshot
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	if e.Encode(kv.db) != nil ||
		e.Encode(kv.prevCmd) != nil ||
		e.Encode(kv.submitCmd) != nil ||
		e.Encode(kv.history) != nil ||
		e.Encode(kv.matchIndex) != nil {
		log.Fatalf("[%v]Encode KVServer State Failed", kv.getServerDetail())
	}

	go kv.rf.Snapshot(int(kv.appliedLogIdx), buf.Bytes())
}

func (kv *ShardKV) monitorTerm() {
	for !kv.killed() {
		term, _ := kv.rf.GetState()
		if int32(term) != kv.getRaftTerm() {
			DPrintf("[%s]Raft Term Change:%d-->%d", kv.getServerDetail(), kv.getRaftTerm(), term)
			kv.setRaftTerm(int32(term))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.killCh <- true
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.shardCtrlerClerk = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.raftPersister = persister
	kv.killCh = make(chan bool)
	kv.db = make(map[string]string)
	kv.history = make(map[int64]map[int32]string)
	kv.matchIndex = make(map[int64]int32)
	kv.submitCmd = make(map[int]*Command)
	kv.prevCmd = make(map[int64]*Command)

	go kv.ticker()
	go kv.monitorTerm()

	return kv
}
