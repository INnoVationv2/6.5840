package kvraft

import "fmt"

const (
	FAILED = int32(iota)
	OK
	ErrorNotLeader
)

type Err string

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
