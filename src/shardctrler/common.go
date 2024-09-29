package shardctrler

import (
	"fmt"
)

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

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

type Reply interface {
	getErr() Err
	String() string
}

type JoinArgs struct {
	ClientId  int64
	CommandId int32

	Servers map[int][]string // new GID -> servers mappings
}

func (args *JoinArgs) String() string {
	return fmt.Sprintf("{JoinArgs ClientId:%d,CmdId:%d,Servers:%v}", args.ClientId, args.CommandId, args.Servers)
}

func (args *JoinArgs) GetCommandId() int32 {
	return args.CommandId
}

type JoinReply struct {
	Err Err
}

func (reply *JoinReply) getErr() Err {
	return reply.Err
}

func (reply *JoinReply) String() string {
	return fmt.Sprintf("{JoinReply Err:%v}", reply.Err)
}

type LeaveArgs struct {
	ClientId  int64
	CommandId int32

	GIDs []int
}

func (args *LeaveArgs) String() string {
	return fmt.Sprintf("{LeaveArgs ClientId:%d,CmdId:%d,GIDs:%v}", args.ClientId, args.CommandId, args.GIDs)
}

func (args *LeaveArgs) GetCommandId() int32 {
	return args.CommandId
}

type LeaveReply struct {
	Err Err
}

func (reply *LeaveReply) getErr() Err {
	return reply.Err
}

func (reply *LeaveReply) String() string {
	return fmt.Sprintf("{LeaveReply Err:%v}", reply.Err)
}

type MoveArgs struct {
	ClientId  int64
	CommandId int32

	Shard int
	GID   int
}

func (args *MoveArgs) String() string {
	return fmt.Sprintf("{LeaveArgs ClientId:%d,CmdId:%d,Shard:%d,GID:%d}", args.ClientId, args.CommandId, args.Shard, args.GID)
}

func (args *MoveArgs) GetCommandId() int32 {
	return args.CommandId
}

type MoveReply struct {
	Err Err
}

func (reply *MoveReply) getErr() Err {
	return reply.Err
}

func (reply *MoveReply) String() string {
	return fmt.Sprintf("{MoveReply Err:%v}", reply.Err)
}

type QueryArgs struct {
	ClientId  int64
	CommandId int32

	Num int // desired config number
}

func (args *QueryArgs) String() string {
	return fmt.Sprintf("{QueryArgs ClientId:%d,CmdId:%d,Num:%d}", args.ClientId, args.CommandId, args.Num)
}

func (args *QueryArgs) GetCommandId() int32 {
	return args.CommandId
}

type QueryReply struct {
	Err    Err
	Config Config
}

func (reply *QueryReply) getErr() Err {
	return reply.Err
}

func (reply *QueryReply) String() string {
	return fmt.Sprintf("{QueryReply Err:%v, Config:%v}", reply.Err, reply.Config)
}

type Pair struct {
	gid int
	cnt int
}
