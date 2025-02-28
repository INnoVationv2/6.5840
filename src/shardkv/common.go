package shardkv

import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

type CmdType int

const (
	Get = iota
	Put
	Append
)

func (c CmdType) String() string {
	switch c {
	case Get:
		return "Get"
	case Put:
		return "Put"
	case Append:
		return "Append"
	default:
		return "Unknown CmdType"
	}
}

type Args struct {
	ClientId  int64
	CommandId int32

	Key   string
	Value string
}

func (ck *Clerk) buildGetArg(key string) *Args {
	return &Args{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Key:       key,
	}
}

func (ck *Clerk) buildPutAppendArg(key, val string) *Args {
	return &Args{
		ClientId:  ck.id,
		CommandId: ck.getCmdId(),
		Key:       key,
		Value:     val,
	}
}

func (args *Args) String() string {
	return fmt.Sprintf("{Arg ClientId:%d,CmdId:%d,Key:%s,Val:%s}", args.ClientId, args.CommandId, args.Key, args.Value)
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
	set(key, val string)
	get(key string) string
	append(key, val string) string
	export() map[string]string
	setDB(val map[string]string)
}

type Command struct {
	ClientId int64
	CmdId    int32

	Type  CmdType
	Key   string
	Value string
}

func (cmd *Command) String() string {
	return fmt.Sprintf("{%s %s->%s}", cmd.Type, cmd.Key, cmd.Value)
}

func buildCommand(opType CmdType, arg *Args) *Command {
	return &Command{
		ClientId: arg.ClientId,
		CmdId:    arg.CommandId,
		Type:     opType,
		Key:      arg.Key,
		Value:    arg.Value}
}
