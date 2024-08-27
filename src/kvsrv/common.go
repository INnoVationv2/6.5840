package kvsrv

import "fmt"

type PutAppendArgs struct {
	ClientID int64

	ID    int32
	Key   string
	Value string
}

func (arg PutAppendArgs) string() string {
	return fmt.Sprintf("{id:%d, clientID:%d, key:%v, value:%v}", arg.ID, arg.ClientID, arg.Key, arg.Value)
}

type GetArgs struct {
	ClientID int64

	Key string
}

func (arg GetArgs) string() string {
	return fmt.Sprintf("{clientID: %d, key: %v}", arg.ClientID, arg.Key)
}

type ReportArgs struct {
	ClientID int64
	Key      string
}

type PutAppendReply struct {
	Value string
}

type GetReply struct {
	Value string
}

type ReportReply struct{}
