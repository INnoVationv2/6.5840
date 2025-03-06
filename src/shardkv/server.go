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

	submittedShardConfNum int
}

func (kv *ShardKV) getCmdId() int32 {
	return atomic.AddInt32(&kv.commandCnt, 1)
}

func (kv *ShardKV) checkShard(key string) Status {
	shardNum, shardConf := key2shard(key), kv.getShardConfig()

	DPrintf("[%v]CheckShard Key:%s,Shard[%d],BelongGID:%d,ShardStatus:%v,ShardConf:%v", kv.getServerDetail(), key, shardNum, shardConf.Shards[shardNum], kv.db.getShardStatus(shardNum), shardConf)
	if shardConf.Shards[shardNum] != kv.gid {
		return ErrWrongGroup
	}
	if kv.db.getShardStatus(shardNum) != Available {
		//DPrintf("[%v]CheckShard ShardNum:%d, Status:%v", kv.getServerDetail(), shardNum, kv.db.getShardStatus(shardNum))
		return ErrShardUnavailable
	}
	return OK
}

func (kv *ShardKV) submitNewShardConfig(newConf *shardctrler.Config) {
	msg := fmt.Sprintf("[%v]New Shard Config:%v\n", kv.getServerDetail(), &newConf)
	DPrintf(msg)
	defer DPrintf("%s Complete", msg)

	cmd, reply := kv.buildShardConfigCommand(newConf), &Reply{Status: Failed}
	for reply.Status != OK {
		kv.Submit(cmd, reply)
		if reply.Status != OK {
			cmd.CmdId = kv.getCmdId()
		}
	}
}

func (kv *ShardKV) delShard(cmd *Command) {
	msg := fmt.Sprintf("[%v]Delete Shard[%d]", kv.getServerDetail(), cmd.Shard.ShardNum)
	DPrintf(msg)
	defer DPrintf("%s Complete", msg)

	cmd.Shard.Op = Delete
	cmd.Shard.Data = nil

	reply := &Reply{Status: Failed}
	for reply.Status != OK {
		cmd.CmdId = kv.getCmdId()
		kv.Submit(cmd, reply)
	}
}

func (kv *ShardKV) changeShardStatus(cmd *Command) {
	msg := fmt.Sprintf("[%v]Change Shard[%d] Status To Available", kv.getServerDetail(), cmd.Shard.ShardNum)
	DPrintf(msg)
	defer DPrintf("%s Complete", msg)

	cmd.Shard.Op = ChangeShardStatus
	cmd.Shard.Data = nil

	reply := &Reply{Status: Failed}
	for reply.Status != OK {
		cmd.CmdId = kv.getCmdId()
		kv.Submit(cmd, reply)
	}
}

func (kv *ShardKV) Submit(cmd *Command, reply *Reply) {
	for cmd.Type != ShardConfig && kv.getShardConfig() == nil {
		time.Sleep(time.Millisecond * 10)
	}

	cmdType := cmd.BasicArgs.Type
	DPrintf("[%v]Receive Command:%v", kv.getServerDetail(), cmd)
	// 如果是重复请求，直接返回OK
	if kv.getMatchIndex(cmd.ClientId) >= cmd.CmdId {
		DPrintf("[%s]Duplicate Command:%v", kv.getServerDetail(), cmd)
		reply.Status = OK
		if cmd.Type == Get {
			reply.Value, _ = kv.getHistory(cmd.ClientId, cmd.CmdId)
		}
		return
	}
	kv.submitCmdToRaft(cmd, reply)
	DPrintf("[%v]%s Complete %v->%v", kv.getServerDetail(), cmdType, cmd, reply)
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
				//DPrintf("[%s]Receive Command %v From Raft", kv.getServerDetail(), &cmd)
				kv.applyCommand(&cmd)
				kv.setAppliedLogIdx(cmdIdx)
				kv.checkSnapshot()
			} else {
				DPrintf("[%s]Receive Snapshot From Raft,LastIncludeIdx:%d", kv.getServerDetail(), msg.SnapshotIndex)
				kv.readSnapshot(msg.SnapshotIndex, msg.Snapshot)
			}
		}
	}
}

func (kv *ShardKV) applyCommand(cmd *Command) {
	DPrintf("[%v]Apply Command:%v ", kv.getServerDetail(), cmd)
	var status Status
	var value string

	switch cmd.Type {
	case ShardConfig:
		status = kv.applyShardConfig(cmd.ShardConf)
	case ShardData:
		status = kv.applyShardData(cmd.Shard)
	default:
		status, value = kv.applyKVRequest(cmd)
	}

	if reply, ok := kv.getSubmitCmd(cmd); ok {
		reply.Status = status
		if cmd.Type == Get {
			reply.Value = value
		}
	}

	kv.setMatchIndex(cmd.ClientId, cmd.CmdId)
	DPrintf("[%v]Apply Command:%v Complete.status:%v,value:%v", kv.getServerDetail(), cmd, status, value)
}

func (kv *ShardKV) applyKVRequest(cmd *Command) (Status, string) {
	args := cmd.KVArgs
	if status := kv.checkShard(args.Key); status != OK {
		return status, ""
	}

	if cmd.Type != Get && kv.getMatchIndex(cmd.ClientId) < cmd.CmdId {
		if cmd.Type == Put {
			kv.db.put(args.Key, args.Value)
			DPrintf("[%v]Put %v->%v", kv.getServerDetail(), args.Key, args.Value)
		} else if cmd.Type == Append {
			val := kv.db.append(args.Key, args.Value)
			DPrintf("[%v]Append %v,After Append:%s", kv.getServerDetail(), cmd, val)
		}
	}

	var result string
	if kv.rf.IsLeader() && cmd.Type == Get {
		result = kv.db.get(args.Key)
		kv.setHistory(cmd.ClientId, cmd.CmdId, result)
		DPrintf("[%v]Get %v,%v", kv.getServerDetail(), cmd, result)
	}
	return OK, result
}

func (kv *ShardKV) readSnapshot(lastIncludeIndex int, snapshot []byte) {
	//DPrintf("[%v]lastIncludeIndex:%d AppliedLogIdx:%d", kv.getServerDetail(), lastIncludeIndex, kv.appliedLogIdx)
	if int32(lastIncludeIndex) <= kv.getAppliedLogIdx() {
		return
	}

	kv.setAppliedLogIdx(int32(lastIncludeIndex))
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	var db [shardctrler.NShards]*ShardDetail
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
	//DPrintf("[%v]Maxraftstate:%d RaftStateSize:%d", kv.getServerDetail(), kv.maxraftstate, kv.raftPersister.RaftStateSize())
	if kv.maxraftstate-kv.raftPersister.RaftStateSize() > 50 {
		return
	}
	//DPrintf("[%v]Build Snapshot", kv.getServerDetail())

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
	kv.id = nrand()
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
