package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"time"
)

// 每100ms更新一次Shard Config
// 只有Leader进行监测更新
func (kv *ShardKV) configMonitor() {
	for {
		if kv.rf.IsLeader() {
			kv.updateConfig()
		}

		select {
		case <-kv.shutdownCh:
			return
		case <-time.After(time.Millisecond * 100):
		}
	}
}

func (kv *ShardKV) updateConfig() {
	oldConf, newConf := kv.getShardConfig(), kv.shardCtrler.Query(-1)
	if oldConf == nil || newConf.Num > oldConf.Num {
		DPrintf("[%v]Found New Shard Config:%v\n", kv.getServerDetail(), &newConf)
		kv.setNewShardConf(&newConf)
	}
}

const (
	Add = iota
	Delete
)

type Shard struct {
	Op       int
	ConfNum  int
	ShardNum int
	Data     map[string]string
}

func (s Shard) String() string {
	return fmt.Sprintf("{Shard Type:%v, ConfNum:%d, ShardNum:%d}", s.Op, s.ConfNum, s.ShardNum)
}

type SendShardArgs struct {
	BasicArgs
	SenderGID   int
	ReceiverGID int
	Shard
}

func (kv *ShardKV) applyNewShardConfig(newConf *shardctrler.Config) {
	oldConf := kv.getShardConfig()
	DPrintf("[%v]Apply Shard Config:%v\n", kv.getServerDetail(), newConf)
	kv.setShardConfig(newConf)

	if oldConf == nil {
		for shard := range newConf.Shards {
			kv.db.setShardStatus(shard, Available)
		}
		return
	}

	if !kv.rf.IsLeader() {
		return
	}
	for shard, gid := range newConf.Shards {
		if oldConf.Shards[shard] == kv.gid && gid != kv.gid && kv.db.getShardStatus(shard) == Available {
			args := &SendShardArgs{
				SenderGID:   kv.gid,
				ReceiverGID: gid,
				BasicArgs: BasicArgs{
					ClientId: kv.id,
					CmdId:    kv.getCmdId()},
				Shard: Shard{
					Op:       Add,
					ConfNum:  newConf.Num,
					ShardNum: shard,
					Data:     kv.db.export(shard)},
			}
			kv.db.setShardStatus(shard, Unavailable)
			go kv.sendShardData(args, newConf.Groups[gid])
		}
	}
}

func (kv *ShardKV) sendShardData(args *SendShardArgs, servers []string) {
	msg := fmt.Sprintf("[%v]Send Shard %v To ShardKV %d", kv.getServerDetail(), args.Shard, args.ReceiverGID)
	DPrintf(msg)
	defer DPrintf("%s Complete", msg)

	reply := &Reply{Status: Failed}
	serverNo := 0
	for {
		clientEnd := kv.make_end(servers[serverNo])
		ok := clientEnd.Call("ShardKV.AddShard", args, reply)
		DPrintf("Send Result: %s, %v, %v", msg, ok, reply.Status)
		if ok && reply.Status == OK {
			kv.delShard(args)
			// 删除完成后,向ClientEnd发送确认
			Report(clientEnd, int32(serverNo), &args.BasicArgs)
			return
		}

		if !ok || reply.Status == ErrNotLeader {
			serverNo = (serverNo + 1) % len(servers)
			if serverNo == 0 {
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func (kv *ShardKV) addShard(cmd *Command) Status {
	shard := cmd.ShardData
	shardNum := shard.ShardNum
	shardConf := kv.getShardConfig()
	if shardConf.Num >= shard.ConfNum && shardConf.Shards[shardNum] != kv.gid {
		DPrintf("[%v]ConfNum:%d,Shard %d Belong to GID:%d", kv.getServerDetail(), shard.ConfNum, shardNum, shardConf.Shards[shardNum])
		return ErrWrongGroup
	}

	DPrintf("[%v]Add New Shard %d", kv.getServerDetail(), shardNum)
	kv.db.setShard(shardNum, shard.Data)
	return OK
}

func (kv *ShardKV) deleteShard(cmd *Command) Status {
	shardData := cmd.ShardData
	shard := shardData.ShardNum
	DPrintf("[%v]Delete Shard %d", kv.getServerDetail(), shard)
	kv.db.setShardStatus(shard, Unavailable)
	return OK
}
