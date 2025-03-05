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
}

func (args *BasicArgs) getClientId() int64 {
	return args.ClientId
}

func (args *BasicArgs) getCmdId() int32 {
	return args.CmdId
}

type KVPair struct {
	Key   string
	Value string
}

func (p KVPair) String() string {
	return fmt.Sprintf("Key:%v,Value:%v", p.Key, p.Value)
}

type KVArgs struct {
	BasicArgs
	KVPair
}

func (args *KVArgs) String() string {
	return fmt.Sprintf("{Arg ClientId:%d,CmdId:%d,Args:%v}", args.ClientId, args.CmdId, args.KVPair)
}

func (ck *Clerk) buildGetArg(key string) *KVArgs {
	return &KVArgs{
		BasicArgs: BasicArgs{ClientId: ck.id, CmdId: ck.getCmdId()},
		KVPair:    KVPair{Key: key},
	}
}

func (ck *Clerk) buildPutAppendArg(key, val string) *KVArgs {
	return &KVArgs{
		BasicArgs: BasicArgs{ClientId: ck.id, CmdId: ck.getCmdId()},
		KVPair:    KVPair{Key: key, Value: val},
	}
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
	Type      CmdType
	ClientId  int64
	CmdId     int32
	KVArgs    *KVPair
	ShardConf *shardctrler.Config
	ShardData *Shard
}

func (cmd *Command) String() string {
	var str string
	if cmd.KVArgs != nil {
		str = fmt.Sprintf("KVArgs:%v", cmd.KVArgs)
	} else if cmd.ShardConf != nil {
		str = fmt.Sprintf("ShardConf:%v", cmd.ShardConf)
	} else if cmd.ShardData != nil {
		str = fmt.Sprintf("ShardData:%v", cmd.ShardData)
	}
	return fmt.Sprintf("{Type:%s,ClientId%d,CmdId:%d,%s}", cmd.Type, cmd.ClientId, cmd.CmdId, str)
}

func (kv *ShardKV) buildCommand(opType CmdType, args interface{}) (cmd *Command) {
	cmd = &Command{
		Type:     opType,
		ClientId: kv.id,
		CmdId:    kv.getCmdId(),
	}
	switch opType {
	case ShardConfig:
		cmd.ShardConf = args.(*shardctrler.Config)
	case ShardData:
		cmd.ShardData = &args.(*SendShardArgs).Shard
	default:
		cmd.KVArgs = &args.(*KVArgs).KVPair
	}
	return
}

type Status int

const (
	Failed = iota
	ErrWrongGroup
	ErrNotLeader
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
	default:
		return "Unknown Status"
	}
}

type Reply struct {
	Status Status
	Value  string
}

func (r *Reply) String() string {
	return fmt.Sprintf("{Reply Status:%d,Val:%s}", r.Status, r.Value)
}

type DB interface {
	get(key string) (val string)
	set(key, val string)
	append(key, val string)
	setShard(shard int, val map[string]string)
	setDB(data [shardctrler.NShards]map[string]string)
	export(shard int) (db map[string]string)
	exportAll() (db [shardctrler.NShards]map[string]string)
	getShardStatus(shard int) ShardStatus
	setShardStatus(shard int, status ShardStatus)
}
