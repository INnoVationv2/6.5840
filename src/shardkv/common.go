package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type Args interface {
	getClientId() int64
	getCmdId() int32
}

type BasicArgs struct {
	ClientId int64
	CmdId    int32
	Type     CmdType
}

type KVPair struct {
	Key   string
	Value string
}

func (p KVPair) String() string {
	return fmt.Sprintf("Key:%v,Value:%v", p.Key, p.Value)
}

type CmdType int

const (
	Get = iota
	Put
	Append
	ShardConfig
	ShardData
)

func (c CmdType) String() string {
	switch c {
	case Get:
		return "Get"
	case Put:
		return "Put"
	case Append:
		return "Append"
	case ShardConfig:
		return "ShardConfig"
	case ShardData:
		return "ShardData"
	default:
		return "Unknown CmdType"
	}
}

type Command struct {
	BasicArgs
	KVArgs    *KVPair
	ShardConf *shardctrler.Config
	Shard     *Shard
}

func (cmd *Command) String() string {
	var str string
	if cmd.KVArgs != nil {
		str = fmt.Sprintf("KVArgs:%v", cmd.KVArgs)
	} else if cmd.ShardConf != nil {
		str = fmt.Sprintf("ShardConf:%v", cmd.ShardConf)
	} else if cmd.Shard != nil {
		str = fmt.Sprintf("ShardData:%v", cmd.Shard)
	}
	return fmt.Sprintf("{Type:%s,ClientId:%d,CmdId:%d,%s}", cmd.Type, cmd.ClientId, cmd.CmdId, str)
}

func (ck *Clerk) buildGetCommand(key string) *Command {
	return &Command{
		BasicArgs: BasicArgs{Type: Get, ClientId: ck.id, CmdId: ck.getCmdId()},
		KVArgs:    &KVPair{Key: key},
	}
}

func (ck *Clerk) buildPutCommand(key, val string) *Command {
	return ck.buildPutAppendCommand(Put, key, val)
}

func (ck *Clerk) buildAppendCommand(key, val string) *Command {
	return ck.buildPutAppendCommand(Append, key, val)
}

func (ck *Clerk) buildPutAppendCommand(op CmdType, key, val string) *Command {
	return &Command{
		BasicArgs: BasicArgs{Type: op, ClientId: ck.id, CmdId: ck.getCmdId()},
		KVArgs:    &KVPair{Key: key, Value: val},
	}
}

func (kv *ShardKV) buildShardConfigCommand(shardConf *shardctrler.Config) *Command {
	return &Command{
		BasicArgs: BasicArgs{Type: ShardConfig, ClientId: kv.id, CmdId: kv.getCmdId()},
		ShardConf: shardConf,
	}
}

func (kv *ShardKV) buildShardCommand(op int, shardDetail *ShardDetail) *Command {
	if op == Delete || op == ChangeShardStatus {
		shardDetail.Data = nil
	}
	return &Command{
		BasicArgs: BasicArgs{Type: ShardData, ClientId: kv.id, CmdId: kv.getCmdId()},
		Shard: &Shard{
			Op:          op,
			ShardDetail: *shardDetail,
		},
	}
}

type Status int

const (
	Failed = iota
	ErrWrongGroup
	ErrNotLeader
	ErrShardUnavailable
	OK
)

func (s Status) String() string {
	switch s {
	case Failed:
		return "Failed"
	case ErrWrongGroup:
		return "ErrWrongGroup"
	case ErrNotLeader:
		return "ErrNotLeader"
	case OK:
		return "OK"
	case ErrShardUnavailable:
		return "ErrShardUnavailable"
	default:
		return "Unknown Status"
	}
}

type Reply struct {
	ShardConfTerm int
	Status        Status
	Value         string
}

func (r *Reply) String() string {
	return fmt.Sprintf("{Reply Status:%v,ShardConfTerm:%d,Val:%s}", r.Status, r.ShardConfTerm, r.Value)
}

type DB interface {
	get(key string) (val string)
	put(key, val string)
	append(key, val string) string
	setShard(shard *Shard)
	deleteShard(shardNum, confNum int)
	setDB(data [shardctrler.NShards]*ShardDetail)
	exportShard(shard int) *ShardDetail
	exportAll() (db [shardctrler.NShards]*ShardDetail)
	getShardDetail(shardNum int) (ShardStatus, int)
	getShardStatus(shardNum int) ShardStatus
	setShardStatus(shardNum int, status ShardStatus)
	compareAndSwapShardStatus(shardNum int, oldStatus, newStatus ShardStatus) bool
	getShardConfNum(shardNum int) int
	setShardConfNum(shardNum, confNum int)
	getShardOwnerGid(shardNum int) int
	setShardOwnerGid(shardNum, owner int)
}
