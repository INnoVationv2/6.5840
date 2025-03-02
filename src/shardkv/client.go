package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"fmt"
	"sync/atomic"
	"time"
)

type Clerk struct {
	id         int64
	commandCnt int32

	shardController *shardctrler.Clerk
	shardConfig     *shardctrler.Config
	make_end        func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	leaderId        int32
	gidClientEndMap map[int][]*labrpc.ClientEnd
}

func (ck *Clerk) getCmdId() int32 {
	return atomic.AddInt32(&ck.commandCnt, 1)
}

func (ck *Clerk) Get(key string) string {
	args := ck.buildGetArg(key)
	return ck.callServer("Get", args).Value
}

func (ck *Clerk) Put(key string, value string) {
	args := ck.buildPutAppendArg(key, value)
	ck.callServer("Put", args)
}

func (ck *Clerk) Append(key string, value string) {
	args := ck.buildPutAppendArg(key, value)
	ck.callServer("Append", args)
}

func (ck *Clerk) callServer(op string, args *KVArgs) (reply *Reply) {
	str := fmt.Sprintf("[Client]%s %v", op, args)
	DPrintf(str)
	defer DPrintf("%s Complete", str)

	if ck.shardConfig == nil {
		ck.updateShardConfig()
	}

	shard := key2shard(args.Key)
	gid := ck.shardConfig.Shards[shard]
	kvSrvClientEnds := ck.gidClientEndMap[gid]

	leaderId := atomic.LoadInt32(&ck.leaderId)
	serverNo := leaderId
	reply = &Reply{Status: Failed}
	for {
		DPrintf("[Client]Send %s RPC %v To ShardKV %d", op, args, serverNo)
		ok := kvSrvClientEnds[serverNo].Call("ShardKV."+op, args, reply)
		if !ok || reply.Status == ErrWrongGroup {
			if ck.updateShardConfig() {
				gid = ck.shardConfig.Shards[shard]
				kvSrvClientEnds = ck.gidClientEndMap[gid]
				serverNo = 0
			}
			continue
		}

		// 下面都是OK
		switch reply.Status {
		case OK:
			go Report(kvSrvClientEnds[serverNo], serverNo, &args.BasicArgs)
			atomic.StoreInt32(&ck.leaderId, serverNo)
			return
		case Failed:
			DPrintf("[Client]CallServer %v Failed, Retrying...", args)
		case ErrNotLeader:
			serverNo = (serverNo + 1) % int32(len(kvSrvClientEnds))
			if serverNo == leaderId {
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func (ck *Clerk) updateShardConfig() bool {
	oldConf, newConf := ck.shardConfig, ck.shardController.Query(-1)
	if oldConf != nil && oldConf.Num == newConf.Num {
		return false
	}

	gidClientEndMap := make(map[int][]*labrpc.ClientEnd)
	for _, gid := range newConf.Shards {
		if _, ok := gidClientEndMap[gid]; ok {
			continue
		}
		for _, server := range newConf.Groups[gid] {
			clientEnd := ck.make_end(server)
			gidClientEndMap[gid] = append(gidClientEndMap[gid], clientEnd)
		}
	}
	DPrintf("[Client]Update Shard Config: New ShardConfig:%v", &newConf)
	ck.shardConfig = &newConf
	ck.gidClientEndMap = gidClientEndMap
	return true
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.shardController = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.id = nrand()
	return ck
}
