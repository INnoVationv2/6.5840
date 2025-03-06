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
			DPrintf("[%v]Is Leader,Updating Shard Config", kv.getServerDetail())
			kv.updateConfig()
			kv.sendShard()
		}

		select {
		case <-kv.shutdownCh:
			return
		case <-time.After(time.Millisecond * 100):
		}
	}
}

func (kv *ShardKV) updateConfig() bool {
	newConf := kv.shardCtrler.Query(-1)
	if newConf.Num > kv.submittedShardConfNum {
		kv.submitNewShardConfig(&newConf)
		kv.submittedShardConfNum = newConf.Num
		return true
	}
	return false
}

func (kv *ShardKV) applyShardConfig(newConf *shardctrler.Config) Status {
	DPrintf("[%v]Apply New Shard Config:%v", kv.getServerDetail(), newConf)
	oldConf := kv.getShardConfig()
	if oldConf != nil && oldConf.Num >= newConf.Num {
		return OK
	}

	kv.setShardConfig(newConf)
	// Update Shard Conf Num
	for shardNum, gid := range newConf.Shards {
		kv.db.setShardOwnerGid(shardNum, gid)
		DPrintf("[%v]Set Shard[%d] Owner To %d", kv.getServerDetail(), shardNum, gid)
		kv.db.setShardConfNum(shardNum, newConf.Num)
		if gid == kv.gid {
			if newConf.Num == 1 {
				kv.db.setShardStatus(shardNum, Available)
				DPrintf("[%v]Set Shard[%d] Status To %v", kv.getServerDetail(), shardNum, Available)
			}
		} else if oldConf != nil && oldConf.Shards[shardNum] == kv.gid && kv.db.getShardStatus(shardNum) == Available {
			// 隐含条件gid != kv.gid; 之前属于我,现在不属于我,需将其发送出去
			kv.db.setShardStatus(shardNum, WaitSend)
			DPrintf("[%v]Set Shard[%d] Status To %v", kv.getServerDetail(), shardNum, WaitSend)
		}
	}
	return OK
}

func (kv *ShardKV) applyShardData(shard *Shard) (status Status) {
	switch shard.Op {
	case Add:
		status = kv.addShard(shard)
	case Delete:
		status = kv.deleteShard(shard)
	case ChangeShardStatus:
		status = kv.updateShardStatus(shard)
	}
	return
}

func (kv *ShardKV) sendShard() {
	shardConf := kv.getShardConfig()
	if shardConf == nil {
		return
	}

	pendingShards := make(map[int][]int)
	for shardNum, gid := range shardConf.Shards {
		if kv.db.getShardStatus(shardNum) == WaitSend && kv.db.compareAndSwapShardStatus(shardNum, WaitSend, Sending) {
			DPrintf("[%v]Set Shard[%d] Status To %v", kv.getServerDetail(), shardNum, Sending)
			pendingShards[gid] = append(pendingShards[gid], shardNum)
		}
	}

	DPrintf("pendingShards:[%v]", pendingShards)
	for _, shards := range pendingShards {
		pendingShardList := shards
		DPrintf("shards:[%v]", pendingShardList)
		go func() {
			for _, shardNum := range pendingShardList {
				shard := kv.db.exportShard(shardNum)
				servers := shardConf.Groups[shard.OwnerGid]
				DPrintf("[%v]Send Shard:%d To %d,Servers:%v,ShardConf:%v", kv.getServerDetail(), shardNum, shard.OwnerGid, servers, shardConf)
				kv.sendShardData(shard, servers)
			}
		}()
	}
}

func (kv *ShardKV) sendShardData(shard *ShardDetail, servers []string) {
	cmd := kv.buildShardCommand(Add, shard)
	shardNum, receiverGid := shard.ShardNum, shard.OwnerGid
	msg := fmt.Sprintf("[%v]Send Shard[%v] To %d, servers:%v", kv.getServerDetail(), shardNum, receiverGid, servers)
	DPrintf(msg)
	defer DPrintf("%s Complete", msg)

Start:
	serverNo := 0

SendShardData:
	reply := &Reply{}
	clientEnd := kv.make_end(servers[serverNo])
	ok := clientEnd.Call("ShardKV.Submit", cmd, reply)
	DPrintf("%s, %v, %v", msg, ok, reply.Status)
	if ok && reply.Status == OK {
		kv.delShard(cmd)
		// 删除完成后,向ClientEnd发送确认
		//Report(clientEnd, int32(serverNo), &cmd.BasicArgs)
		return
	}

	if !ok || reply.Status == ErrNotLeader {
		serverNo = (serverNo + 1) % len(servers)
		if reply.Status == ErrNotLeader {
			cmd.CmdId = kv.getCmdId()
		}
		goto SendShardData
	}
	if reply.Status == ErrWrongGroup {
		goto CheckShardConfig
	}

CheckShardConfig:
	shardConf := kv.getShardConfig()
	receiverGid = shardConf.Shards[shardNum]
	// Shard归属方并没有改变
	if receiverGid == shard.OwnerGid {
		DPrintf("[%v]Shard[%d] Owner Gid Not Changed", kv.getServerDetail(), shardNum)
		return
	}

	DPrintf("[%v]Shard[%d] Owner Gid Change To %d", kv.getServerDetail(), shardNum, shard.OwnerGid)
	// Shard重新属于自己
	if receiverGid == kv.gid {
		kv.changeShardStatus(cmd)
		return
	}
	shard.OwnerGid = receiverGid
	servers = shardConf.Groups[receiverGid]
	goto Start
}

const (
	Add = iota
	Delete
	ChangeShardStatus
)

type Shard struct {
	Op int
	ShardDetail
}

func (s Shard) String() string {
	shardType := ""
	switch s.Op {
	case Add:
		shardType = "Add Shard"
	case Delete:
		shardType = "Delete Shard"
	case ChangeShardStatus:
		shardType = "Change Shard Status"
	}
	return fmt.Sprintf("{%s[%d], ConfNum:%d}", shardType, s.ShardNum, s.ConfNum)
}

func (kv *ShardKV) addShard(shard *Shard) Status {
	shardNum, shardConf := shard.ShardNum, kv.getShardConfig()
	if shardConf.Num >= shard.ConfNum && shardConf.Shards[shardNum] != kv.gid {
		// 有更新的配置，且该配置下这个Shard不属于该Server
		DPrintf("[%v]ConfNum:%d,Shard[%d] Belong to GID:%d", kv.getServerDetail(), shard.ConfNum, shardNum, shardConf.Shards[shardNum])
		return ErrWrongGroup
	}

	DPrintf("[%v]Add Shard[%d]", kv.getServerDetail(), shardNum)
	kv.db.setShard(shard)
	return OK
}

func (kv *ShardKV) updateShardStatus(shard *Shard) Status {
	shardNum, shardConf := shard.ShardNum, kv.getShardConfig()
	if shardConf.Num >= shard.ConfNum && shardConf.Shards[shardNum] != kv.gid {
		DPrintf("[%v]ConfNum:%d,Shard[%d] Belong to GID:%d", kv.getServerDetail(), shard.ConfNum, shardNum, shardConf.Shards[shardNum])
		return ErrWrongGroup
	}

	kv.db.setShardStatus(shardNum, Available)
	return OK
}

func (kv *ShardKV) deleteShard(shard *Shard) Status {
	kv.db.deleteShard(shard.ShardNum, shard.ConfNum)
	return OK
}
