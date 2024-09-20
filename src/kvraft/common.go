package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	Killed         = "Killed"
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

func (args *GetArgs) String() string {
	return fmt.Sprintf("GetArgs:{Key:%s}", args.Key)
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

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("PutAppendArgs:{Key:%s,Val:%s}", args.Key, args.Value)
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
	return fmt.Sprintf("GetReply {Err:%s,Val:%s}", gr.Err, gr.Value)
}

type PutAppendReply struct {
	Err Err
}

func (pr *PutAppendReply) getErr() Err {
	return pr.Err
}

func (pr *PutAppendReply) String() string {
	return fmt.Sprintf("PutAppendReply {Err:%s}", pr.Err)
}
