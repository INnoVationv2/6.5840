package shardctrler

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	id         int64
	commandCnt int32
	servers    []*labrpc.ClientEnd
	leaderId   int32
}

func (ck *Clerk) getCmdId() int32 {
	return atomic.AddInt32(&ck.commandCnt, 1)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.id = nrand()
	ck.servers = servers
	return ck
}

func (ck *Clerk) Query(idx int) Config {
	args := &Args{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Type:      QUERY,
		ConfigIdx: idx,
	}
	DPrintf("[Client]New Command Query %v\n", args)
	reply := ck.callServer("Query", args)
	DPrintf("[Client]Command Query Complete %v:%v", args, reply)
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &Args{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Type:      JOIN,
		Servers:   servers,
	}
	DPrintf("[Client]New Command Join %v\n", args)
	ck.callServer("Join", args)
	DPrintf("[Client]Command Join Complete %v", args)
}

func (ck *Clerk) Leave(gids []int) {
	args := &Args{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Type:      LEAVE,
		GIDs:      gids,
	}
	DPrintf("[Client]New Command Leave %v\n", args)
	ck.callServer("Leave", args)
	DPrintf("[Client]Command Leave Complete %v", args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &Args{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Type:      MOVE,
		Shard:     shard,
		GID:       gid,
	}
	DPrintf("[Client]New Command Move %v\n", args)
	ck.callServer("Move", args)
	DPrintf("[Client]Command Move Complete %v", args)
}

func (ck *Clerk) callServer(op string, args *Args) (reply *Reply) {
	leaderId := ck.leaderId
	serverNo := leaderId
	reply = &Reply{Status: Failed}
	for {
		DPrintf("[Client]Send %s RPC %v To ShardCtrler %d", op, args, serverNo)
		ok := ck.servers[serverNo].Call("ShardCtrler."+op, args, reply)
		if ok && reply.Status == OK {
			go ck.Report(serverNo, args)
			atomic.StoreInt32(&ck.leaderId, serverNo)
			return
		}
		if !ok || reply.Status == ErrNotLeader {
			serverNo = (serverNo + 1) % int32(len(ck.servers))
			if serverNo == leaderId {
				time.Sleep(time.Millisecond * 10)
			}
		}
	}
}

func (ck *Clerk) Report(serverNo int32, arg *Args) {
	args, reply := Args{ClientId: ck.id, CommandId: arg.CommandId}, Reply{}
	DPrintf("[Client]Command %d Is Complete, Send Report RPC To ShardCtrler %d", arg.CommandId, serverNo)
	for ok := false; !ok; {
		ok = ck.servers[serverNo].Call("ShardCtrler.Report", &args, &reply)
	}
}
