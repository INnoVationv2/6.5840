package shardkv

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big

	ctrlers     []*labrpc.ClientEnd
	shardCtrler *shardctrler.Clerk
	shardConf   *shardctrler.Config

	raftPersister *raft.Persister

	db DB

	dead       int32
	shutdownCh chan struct{}
	// Client已提交的Command
	submitCmd map[int64]map[int32]*Reply
	// 记录已经执行的最大的LogEntry的Index
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

func (kv *ShardKV) checkShard(key string) bool {
	for kv.shardConf == nil {
		time.Sleep(time.Millisecond * 10)
	}
	shard := key2shard(key)
	return kv.shardConf.Shards[shard] == kv.gid
}

func (kv *ShardKV) Get(arg *Args, reply *Reply) {
	kv.submitCommand(Get, arg, reply)
}

func (kv *ShardKV) Put(args *Args, reply *Reply) {
	kv.submitCommand(Put, args, reply)
}

func (kv *ShardKV) Append(args *Args, reply *Reply) {
	kv.submitCommand(Append, args, reply)
}

func (kv *ShardKV) submitCommand(op CmdType, arg *Args, reply *Reply) {
	DPrintf("[%v]Received %s RPC:%v", kv.getServerDetail(), op, arg)

	if !kv.checkShard(arg.Key) {
		DPrintf("Key %s Not Belong To KVServer %d", arg.Key, kv.gid)
		reply.Status = ErrWrongGroup
		return
	}

	clientId, cmdId := arg.ClientId, arg.CommandId
	// 如果是重复请求，直接返回OK
	if val, ok := kv.getHistory(clientId, cmdId); ok {
		DPrintf("[%s]Dupliacte %s Reuqest %v", kv.getServerDetail(), op, arg)
		reply.Status = OK
		reply.Value = val
		return
	}
	cmd := buildCommand(op, arg)

	kv.addSubmitCmd(cmd, reply)
	defer kv.deleteSubmitCmd(cmd)

	kv.submitCmdToRaft(cmd, reply)
	if reply.Status != OK {
		DPrintf("[%v]Command %v failed:%v", kv.getServerDetail(), cmd, reply.Status)
		return
	}
	DPrintf("[%v]%s Complete %v->%v", kv.getServerDetail(), op, arg, reply)
}

func (kv *ShardKV) submitCmdToRaft(cmd *Command, reply *Reply) {
	DPrintf("[%v]Submit Command %v To Raft", kv.getServerDetail(), cmd)
	defer DPrintf("[%v]Submit Command %v To Raft Complete", kv.getServerDetail(), cmd)

	cmdIdx, raftTerm, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		DPrintf("[%v]Not Leader", kv.getServerDetail())
		reply.Status = ErrNotLeader
		return
	}
	DPrintf("[%v]Success Submit Command %v To Raft, CmdIdx:%d", kv.getServerDetail(), cmd, cmdIdx)

	for !kv.killed() && kv.getRaftTerm() <= raftTerm && kv.getAppliedLogIdx() < int32(cmdIdx) {
	}
}

func (kv *ShardKV) ticker() {
	DPrintf("[%s]Start KVServer.Ticker", kv.getServerDetail())
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%s]Stop KVServer.Ticker", kv.getServerDetail())
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				cmdIdx, cmd := int32(msg.CommandIndex), msg.Command.(Command)
				DPrintf("[%s]Receive Command %v From Raft applyChan", kv.getServerDetail(), &cmd)
				kv.applyCommand(&cmd)
				kv.setAppliedLogIdx(cmdIdx)
				kv.checkSnapshot()
			} else {
				DPrintf("[%s]Receive Snapshot From Raft applyChan, LastIncludeIdx:%d", kv.getServerDetail(), msg.SnapshotIndex)
				kv.readSnapshot(msg.SnapshotIndex, msg.Snapshot)
			}
		}
	}
}

func (kv *ShardKV) applyCommand(cmd *Command) {
	DPrintf("[%v]Apply %v ", kv.getServerDetail(), cmd)
	defer DPrintf("[%v]Apply %v Complete", kv.getServerDetail(), cmd)

	if cmd.Type != Get && kv.matchIndex[cmd.ClientId] < cmd.CmdId {
		switch cmd.Type {
		case Put:
			kv.db.set(cmd.Key, cmd.Value)
			DPrintf("[%v][ApplyCommand]Put %v->%v", kv.getServerDetail(), cmd, cmd.Value)
		case Append:
			value := kv.db.append(cmd.Key, cmd.Value)
			DPrintf("[%v][ApplyCommand]Append %v->%v", kv.getServerDetail(), cmd, value)
		}
	}

	value := ""
	if cmd.Type == Get {
		value = kv.db.get(cmd.Key)
		DPrintf("[%v][ApplyCommand]Get %v->%v", kv.getServerDetail(), cmd, value)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.setHistory(cmd.ClientId, cmd.CmdId, value)
	if cmd.CmdId > kv.matchIndex[cmd.ClientId] {
		kv.matchIndex[cmd.ClientId] = cmd.CmdId
	}
	if reply, ok := kv.submitCmd[cmd.ClientId][cmd.CmdId]; ok {
		reply.Status = OK
		reply.Value = value
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
		e.Encode(kv.submitCmd) != nil ||
		e.Encode(kv.history) != nil ||
		e.Encode(kv.matchIndex) != nil {
		log.Fatalf("[%v]Encode ShardKV State Failed", kv.getServerDetail())
	}

	go kv.rf.Snapshot(int(kv.appliedLogIdx), buf.Bytes())
}

func (kv *ShardKV) updateShardConfig() {
	for {
		oldConf, newConf := kv.getShardConfig(), kv.shardCtrler.Query(-1)
		if oldConf == nil || newConf.Num != oldConf.Num {
			kv.setShardConfig(&newConf)
		}
		select {
		case <-kv.shutdownCh:
			return
		case <-time.After(time.Millisecond * 100):
		}
	}
}

func (kv *ShardKV) Kill() {
	close(kv.shutdownCh)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) Report(args *Args, reply *Reply) {
	DPrintf("[%v]Receive Report RPC %v, Delete History", kv.getServerDetail(), args)
	kv.deleteHistory(args.ClientId, args.CommandId)
	reply.Status = OK
}

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
	kv.shardCtrler = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.raftPersister = persister
	kv.shutdownCh = make(chan struct{})
	kv.db = buildInMemoryDB()
	kv.history = make(map[int64]map[int32]string)
	kv.matchIndex = make(map[int64]int32)
	kv.submitCmd = make(map[int64]map[int32]*Reply)

	go kv.ticker()
	go kv.updateShardConfig()

	return kv
}
