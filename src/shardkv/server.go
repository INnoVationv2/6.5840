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

	id         int64
	commandCnt int32
}

func (kv *ShardKV) getCmdId() int32 {
	return atomic.AddInt32(&kv.commandCnt, 1)
}

func (kv *ShardKV) checkIfCommandAlreadyExecuted(clientId int64, commandId int32) bool {
	if kv.matchIndex[clientId] >= commandId {
		return true
	}
	return false
}

func (kv *ShardKV) checkShard(key string) Status {
	shard := key2shard(key)
	if kv.getShardConfig().Shards[shard] != kv.gid {
		return ErrWrongGroup
	} else if kv.db.getShardStatus(shard) != Available {
		return Failed
	}
	return OK
}

func (kv *ShardKV) Get(arg *KVArgs, reply *Reply) {
	kv.submit(Get, arg, reply)
}

func (kv *ShardKV) Put(args *KVArgs, reply *Reply) {
	kv.submit(Put, args, reply)
}

func (kv *ShardKV) Append(args *KVArgs, reply *Reply) {
	kv.submit(Append, args, reply)
}

func (kv *ShardKV) AddShard(args *SendShardArgs, reply *Reply) {
	kv.submit(ShardData, args, reply)
}

// 由Leader所在的KVServer发起, 向所有KVServer同步ShardConfig
func (kv *ShardKV) setNewShardConf(newConf *shardctrler.Config) {
	cmd := kv.buildCommand(ShardConfig, newConf)
	DPrintf("[%v]Submit Command %v To Raft", kv.getServerDetail(), cmd)
	kv.rf.Start(*cmd)
}

func (kv *ShardKV) delShard(args *SendShardArgs) {
	args.Shard.Op = Delete
	args.Shard.Data = nil
	cmd := kv.buildCommand(ShardData, args)
	kv.rf.Start(*cmd)
}

func (kv *ShardKV) submit(op CmdType, arg Args, reply *Reply) {
	for kv.getShardConfig() == nil {
		time.Sleep(time.Millisecond * 10)
	}

	DPrintf("[%v]Received %s RPC:%v", kv.getServerDetail(), op, arg)
	clientId, cmdId := arg.getClientId(), arg.getCmdId()
	// 如果是重复请求，直接返回OK
	if val, ok := kv.getHistory(clientId, cmdId); ok {
		DPrintf("[%s]Dupliacte %s Reuqest %v", kv.getServerDetail(), op, arg)
		reply.Status = OK
		reply.Value = val
		return
	}
	cmd := kv.buildCommand(op, arg)
	kv.submitCmdToRaft(cmd, reply)
	DPrintf("[%v]%s Complete %v->%v", kv.getServerDetail(), op, arg, reply)
}

func (kv *ShardKV) submitCmdToRaft(cmd *Command, reply *Reply) {
	DPrintf("[%v]Submit Command %v To Raft", kv.getServerDetail(), cmd)

	kv.addSubmitCmd(cmd, reply)
	defer kv.deleteSubmitCmd(cmd)

	cmdIdx, raftTerm, isLeader := kv.rf.Start(*cmd)
	if !isLeader {
		DPrintf("[%v]Not Leader", kv.getServerDetail())
		reply.Status = ErrNotLeader
		return
	}
	DPrintf("[%v]Success Submit Command %v To Raft, CmdIdx:%d", kv.getServerDetail(), cmd, cmdIdx)

	for !kv.killed() && kv.rf.GetTerm() <= raftTerm && kv.getAppliedLogIdx() < int32(cmdIdx) {
	}

	if reply.Status != OK {
		DPrintf("[%v]Command %v failed:%v", kv.getServerDetail(), cmd, reply.Status)
		return
	}
}

func (kv *ShardKV) ticker() {
	DPrintf("[%s]Start ShardKV", kv.getServerDetail())
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
	if cmd.Type == ShardConfig {
		kv.applyNewShardConfig(cmd.ShardConf)
		return
	}

	reply, ok := kv.getSubmitCmd(cmd)

	if cmd.Type == ShardData {
		var status Status
		switch cmd.ShardData.Op {
		case Add:
			status = kv.addShard(cmd)
		case Delete:
			status = kv.deleteShard(cmd)
		}
		if ok {
			reply.Status = status
		}
		return
	}

	args := cmd.KVArgs
	if status := kv.checkShard(args.Key); status != OK && ok {
		reply.Status = status
		return
	}

	if cmd.Type != Get && kv.matchIndex[cmd.ClientId] < cmd.CmdId {
		switch cmd.Type {
		case Put:
			kv.db.set(args.Key, args.Value)
			//DPrintf("[%v][ApplyCommand]Put %v->%v", kv.getServerDetail(), args.Key, args.Value)
		case Append:
			kv.db.append(args.Key, args.Value)
			//DPrintf("[%v][ApplyCommand]Append %v", kv.getServerDetail(), cmd)
		}
	}

	value := ""
	if cmd.Type == Get {
		value = kv.db.get(args.Key)
		//DPrintf("[%v][ApplyCommand]Get %v->%v", kv.getServerDetail(), cmd, value)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.setHistory(cmd.ClientId, cmd.CmdId, value)
	if cmd.CmdId > kv.matchIndex[cmd.ClientId] {
		kv.matchIndex[cmd.ClientId] = cmd.CmdId
	}
	if ok {
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

	kv.mu.Lock()
	defer kv.mu.Unlock()
	var db [shardctrler.NShards]map[string]string
	if d.Decode(&db) != nil ||
		d.Decode(&kv.submitCmd) != nil ||
		d.Decode(&kv.history) != nil ||
		d.Decode(&kv.matchIndex) != nil ||
		d.Decode(&kv.shardConf) != nil {
		log.Fatalf("[%v]Decode Raft State Failed", kv.getServerDetail())
	}
	kv.db.setDB(db)
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

	kv.mu.RLock()
	// 大小接近，进行snapshot
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	if e.Encode(kv.db.exportAll()) != nil ||
		e.Encode(kv.submitCmd) != nil ||
		e.Encode(kv.history) != nil ||
		e.Encode(kv.matchIndex) != nil ||
		e.Encode(kv.shardConf) != nil {
		log.Fatalf("[%v]Encode ShardKV State Failed", kv.getServerDetail())
	}
	kv.mu.RUnlock()

	kv.rf.Snapshot(int(kv.appliedLogIdx), buf.Bytes())
}

func (kv *ShardKV) Kill() {
	DPrintf("[%v]Kill ShardKV\n", kv.getServerDetail())
	close(kv.shutdownCh)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *ShardKV) Report(args *BasicArgs, reply *Reply) {
	DPrintf("[%v]Receive Report RPC %v, Delete History", kv.getServerDetail(), args)
	kv.deleteHistory(args.ClientId, args.CmdId)
	reply.Status = OK
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	kv := new(ShardKV)
	kv.me = me
	kv.id = int64(me)
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
	go kv.configMonitor()

	return kv
}
