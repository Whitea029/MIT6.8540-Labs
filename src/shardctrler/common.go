package shardctrler

import (
	"fmt"
	"time"
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

const ExecuteTimeout = 500 * time.Millisecond

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func DefaultConfig() Config {
	return Config{
		Groups: make(map[int][]string),
	}
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type OpType uint8

const (
	OpJoin OpType = iota
	OpLeave
	OpMove
	OpQuery
)

func (op OpType) String() string {
	switch op {
	case OpJoin:
		return "Join"
	case OpLeave:
		return "Leave"
	case OpMove:
		return "Move"
	case OpQuery:
		return "Query"
	default:
		panic(fmt.Sprintf("unknown operation type %d", op))
	}
}

type Err uint8

const (
	OK Err = iota
	ErrWrongLeader
	ErrTimeout
)

func (e Err) String() string {
	switch e {
	case OK:
		return "OK"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	default:
		panic(fmt.Sprintf("unknown error type %d", e))
	}
}

type CommandArgs struct {
	// Join args
	Servers map[int][]string
	// Leave args
	GIDs []int
	// Move args
	Shard int
	GID   int
	// Query args
	Num int
	// Common args
	Op        OpType
	ClientId  int64
	CommandId int64
}

func (args *CommandArgs) String() string {
	return fmt.Sprintf("Op: %s, ClientId: %d, CommandId: %d", args.Op, args.ClientId, args.CommandId)
}

type CommandReply struct {
	Err    Err
	Config Config
}

func (reply *CommandReply) String() string {
	return fmt.Sprintf("Err: %s, Config: %v", reply.Err, reply.Config)
}

type OpContext struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

// CommandArgs 是给 client 使用
// Command 是给 apply 使用
type Command struct {
	*CommandArgs
}
