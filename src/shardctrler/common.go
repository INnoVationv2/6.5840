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

func (c *Config) String() string {
	return fmt.Sprintf("{Config Num:%d,Shards:%v,Groups:%v}", c.Num, c.Shards, c.Groups)
}

const (
	Failed = iota
	ErrNotLeader
	OK
)

type Status int

const (
	JOIN = iota
	LEAVE
	MOVE
	QUERY
)

type CmdType int

func (c CmdType) String() string {
	switch c {
	case JOIN:
		return "GET"
	case LEAVE:
		return "PUT"
	case MOVE:
		return "APPEND"
	case QUERY:
		return "QUERY"
	default:
		return "UNKNOWN"
	}
}

type Args struct {
	ClientId  int64
	CommandId int32

	Type CmdType
	// JoinArg
	Servers map[int][]string // new GID -> servers mappings
	// LeaveArg
	GIDs []int
	// MoveArg
	Shard int
	GID   int
	// QueryArg
	ConfigIdx int
}

func (args *Args) String() string {
	switch args.Type {
	case JOIN:
		return fmt.Sprintf("{ArgType:Join ClientId:%d,CmdId:%d,Servers:%v}", args.ClientId, args.CommandId, args.Servers)
	case LEAVE:
		return fmt.Sprintf("{ArgType:LEAVE ClientId:%d,CmdId:%d,GIDs:%v}", args.ClientId, args.CommandId, args.GIDs)
	case MOVE:
		return fmt.Sprintf("{ArgType:Move ClientId:%d,CmdId:%d,Shard:%d,GID:%d}", args.ClientId, args.CommandId, args.Shard, args.GID)
	case QUERY:
		return fmt.Sprintf("{ArgType:QUERY ClientId:%d,CmdId:%d,ConfigIdx:%d}", args.ClientId, args.CommandId, args.ConfigIdx)
	}
	return ""
}

type Reply struct {
	Status Status
	Config Config
}

func (r *Reply) String() string {
	return fmt.Sprintf("{QueryReply Err:%v, Config:%v}", r.Status, r.Config)
}

type Pair struct {
	gid int
	cnt int
}

type DB interface {
	get(idx int) *Config
	size() int
	append(conf *Config)
}
