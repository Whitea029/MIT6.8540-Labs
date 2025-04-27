package kvraft

import (
	"fmt"
	"log"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ExecuteTimeout = 1000 * time.Millisecond

type Err uint8

const (
	Ok Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
)

func (e Err) String() string {
	switch e {
	case Ok:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	default:
		return "Unknown Error"
	}
}

type OpType uint8

const (
	OpPut OpType = iota
	OpAppend
	OpGet
)

func (o OpType) String() string {
	switch o {
	case OpPut:
		return "OpPut"
	case OpAppend:
		return "OpAppend"
	case OpGet:
		return "OpGet"
	default:
		return "Unknown Operation"
	}
}

type CommandArgs struct {
	Key       string
	Value     string
	Op        OpType
	ClientId  int64
	CommandId int64
}

func (args CommandArgs) String() string {
	return fmt.Sprintf("{Key:%v, Value:%v, Op:%v, ClientId:%v, Id:%v}", args.Key, args.Value, args.Op, args.ClientId, args.CommandId)
}

type CommandReply struct {
	Err   Err
	Value string
}

func (reply CommandReply) String() string {
	return fmt.Sprintf("{Err:%v, Value:%v}", reply.Err, reply.Value)
}

type Command struct {
	*CommandArgs
}
