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
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	id         int64
	commandCnt int32

	shardController *shardctrler.Clerk
	shardConfig     *shardctrler.Config
	make_end        func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	leaderId int32
	servers  map[int32][]*labrpc.ClientEnd
}

func (ck *Clerk) getCmdId() int32 {
	return atomic.AddInt32(&ck.commandCnt, 1)
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := &Args{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Key:       key,
	}

	reply := ck.callServer("Get", args)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	args := &Args{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Key:       key,
		Value:     value,
	}

	ck.callServer("Put", args)
}

func (ck *Clerk) Append(key string, value string) {
	args := &Args{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Key:       key,
		Value:     value,
	}

	ck.callServer("Append", args)
}

func (ck *Clerk) callServer(op string, args *Args) (reply *Reply) {
	if ck.shardConfig == nil {
		ck.updateShardConfig()
	}

	shard := key2shard(args.Key)
	gid := int32(ck.shardConfig.Shards[shard])
	servers := ck.servers[gid]

	leaderId := atomic.LoadInt32(&ck.leaderId)
	serverNo := leaderId
	reply = &Reply{Status: Failed}
	for {
		DPrintf("[Client]Send %s RPC %v To ShardKV %d", op, args, serverNo)
		ok := servers[serverNo].Call("ShardKV."+op, args, reply)
		if ok {
			if reply.Status == OK {
				go ck.Report(gid, serverNo, args)
				atomic.StoreInt32(&ck.leaderId, serverNo)
				return
			} else if reply.Status == ErrWrongGroup {
				DPrintf("[Client]Error Wrong Group")
				time.Sleep(100 * time.Millisecond)
				if ck.updateShardConfig() {
					gid = int32(ck.shardConfig.Shards[shard])
					servers = ck.servers[gid]
					serverNo = 0
				}
				continue
			}
		}

		if !ok || reply.Status == ErrNotLeader {
			serverNo = (serverNo + 1) % int32(len(servers))
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

	gidClientEndMap := make(map[int32][]*labrpc.ClientEnd)
	for _, gid := range newConf.Shards {
		gidI32 := int32(gid)
		if _, ok := gidClientEndMap[gidI32]; ok {
			continue
		}
		for _, server := range newConf.Groups[gid] {
			clientEnd := ck.make_end(server)
			gidClientEndMap[gidI32] = append(gidClientEndMap[gidI32], clientEnd)
		}
	}
	DPrintf("[Client]Update Shard Config:\n  New ShardConfig:%v\n  New clientEnds:%v\n", &newConf, gidClientEndMap)
	ck.shardConfig = &newConf
	ck.servers = gidClientEndMap
	return true
}

func (ck *Clerk) Report(gid, serverNo int32, arg *Args) {
	args, reply := Args{ClientId: ck.id, CommandId: arg.CommandId}, Reply{}
	DPrintf("[Client]Command %d Is Complete, Send Report RPC To ShardCtrler %d", arg.CommandId, serverNo)
	for ok := false; !ok; {
		ok = ck.servers[gid][serverNo].Call("ShardKV.Report", &args, &reply)
	}
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.shardController = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.id = nrand()
	return ck
}
