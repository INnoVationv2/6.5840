package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"

	Killed      = "Killed"
	LogNotMatch = "LogNotMatch"
	TermChanged = "TermChanged"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientId  int64
	CommandId int32

	Key   string
	Value string
	Op    string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId  int64
	CommandId int32

	Key string
}

type GetReply struct {
	Err   Err
	Value string
}
