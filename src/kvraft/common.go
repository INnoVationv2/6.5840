package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	Killed         = "Killed"
	LogNotMatch    = "LogNotMatch"
	TermChanged    = "TermChanged"
)

type Err string

type Args interface {
	String() string
	GetCommandId() int32
}

type GetArgs struct {
	ClientId  int64
	CommandId int32

	Key string
}

func (ck *Clerk) buildGetArg(key string) *GetArgs {
	return &GetArgs{
		ClientId:  ck.id,
		CommandId: ck.getCommandId(),
		Key:       key,
	}
}

func (args *GetArgs) String() string {
	return fmt.Sprintf("{Get %s,ClientId:%d,CmdId:%d}", args.Key, args.ClientId, args.CommandId)
}

func (args *GetArgs) GetCommandId() int32 {
	return args.CommandId
}

type PutAppendArgs struct {
	ClientId  int64
	CommandId int32

	Key   string
	Value string
}

func (ck *Clerk) buildPutAppendArg(key, value string) *PutAppendArgs {
	return &PutAppendArgs{
		ClientId:  ck.id,
		CommandId: ck.getCommandId(),
		Key:       key,
		Value:     value,
	}
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("{Key:%s,Val:%s,ClientId:%d,CmdId:%d}", args.Key, args.Value, args.ClientId, args.CommandId)
}

func (args *PutAppendArgs) GetCommandId() int32 {
	return args.CommandId
}

type Reply interface {
	getErr() Err
	String() string
}

type GetReply struct {
	Err   Err
	Value string
}

func (gr *GetReply) getErr() Err {
	return gr.Err
}

func (gr *GetReply) String() string {
	return fmt.Sprintf("{GetReply Err:%s,Val:%s}", gr.Err, gr.Value)
}

type PutAppendReply struct {
	Err Err
}

func (pr *PutAppendReply) getErr() Err {
	return pr.Err
}

func (pr *PutAppendReply) String() string {
	return fmt.Sprintf("{PutAppendReply Err:%s}", pr.Err)
}
