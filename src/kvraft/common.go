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
}

type GetArgs struct {
	Key string
}

func (k *GetArgs) String() string {
	return fmt.Sprintf("GetArgs:{Key:%s}", k.Key)
}

type PutAppendArgs struct {
	Key   string
	Value string
}

func (kv *PutAppendArgs) String() string {
	return fmt.Sprintf("PutAppendArgs:{Key:%s,Val:%s}", kv.Key, kv.Value)
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
