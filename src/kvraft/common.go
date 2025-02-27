package kvraft

import "fmt"

const (
	GET CmdType = iota
	PUT
	APPEND
)

type CmdType int

func (c CmdType) String() string {
	switch c {
	case GET:
		return "GET"
	case PUT:
		return "PUT"
	case APPEND:
		return "APPEND"
	default:
		return "UNKNOWN"
	}
}

type Command struct {
	ClientId int64
	CmdId    int32

	Type  CmdType
	Key   string
	Value string
}

func (cmd *Command) String() string {
	return fmt.Sprintf("{ClientId:%d,CmdId:%d,Type:%v,Key:%v,Value:%v}", cmd.ClientId, cmd.CmdId, cmd.Type, cmd.Key, cmd.Value)
}

func buildCommand(opType CmdType, arg *Arg) *Command {
	return &Command{
		ClientId: arg.ClientId,
		CmdId:    arg.CommandId,
		Type:     opType,
		Key:      arg.Key,
		Value:    arg.Value}
}

type Arg struct {
	ClientId  int64
	CommandId int32

	Key   string
	Value string
}

func (ck *Clerk) buildGetArg(key string) *Arg {
	return &Arg{
		ClientId:  ck.id,
		CommandId: ck.getCommandId(),
		Key:       key,
	}
}

func (ck *Clerk) buildPutAppendArg(key, val string) *Arg {
	return &Arg{
		ClientId:  ck.id,
		CommandId: ck.getCommandId(),
		Key:       key,
		Value:     val,
	}
}

func (args *Arg) String() string {
	return fmt.Sprintf("{Args ClientId:%d,CmdId:%d,Key:%s,Val:%s}", args.ClientId, args.CommandId, args.Key, args.Value)
}

const (
	FAILED = int32(iota)
	OK
	ErrorNotLeader
)

type Reply struct {
	Status int32
	Value  string
}

func (gr *Reply) String() string {
	return fmt.Sprintf("{Reply Status:%d,Val:%s}", gr.Status, gr.Value)
}

type DB interface {
	set(key, val string)
	get(key string) string
	append(key, val string) string
	export() map[string]string
	setDB(val map[string]string)
}
