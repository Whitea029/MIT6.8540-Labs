package shardkv

import (
	"fmt"
	"log"
	"time"

	"6.5840/shardctrler"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ExecuteTimeout              = 500 * time.Millisecond // 执行超时时间
	ConfigurationMonitorTimeout = 100 * time.Millisecond // 配置监控超时
	MigrationMonitorTimeout     = 50 * time.Millisecond  // 迁移超时
	GCMonitorTimeout            = 50 * time.Millisecond  // GC监控超时
	EmptyEntryDetectorTimeout   = 200 * time.Millisecond // 空对象检测超时
)

type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
	ErrOutDated
	ErrTimeout
	ErrNotReady
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongGroup:
		return "ErrWrongGroup"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrOutDated:
		return "ErrOutDated"
	case ErrTimeout:
		return "ErrTimeout"
	case ErrNotReady:
		return "ErrNotReady"
	default:
		panic(fmt.Sprintf("Unknown error: %d", err))
	}
}

type ShardStatus uint8

// 每个 shard 的状态
const (
	Serving   ShardStatus = iota // shard 正在对外服务
	Pulling                      // shard 正在从别的 group 拉取数据
	BePulling                    // shard 正在被别的 group 拉取数据
	GCing
)

func (status ShardStatus) String() string {
	switch status {
	case Serving:
		return "Serving"
	case Pulling:
		return "Pulling"
	case BePulling:
		return "BePulling"
	case GCing:
		return "GCing"
	default:
		panic(fmt.Sprintf("Unknown ShardStatus: %d", status))
	}
}

// 指令类型
type CommandType uint8

const (
	Operation     CommandType = iota // 通用早错
	Configuration                    // 配置变更
	InsertShards                     // 插入 Shard
	DeleteShards                     // 删除 Shard
	EmptyShards                      // 空日志条目 保持领导者的状态和活动性，避免在某些情况下集群处于无操作状态
)

func (commandType CommandType) String() string {
	switch commandType {
	case Operation:
		return "Operation"
	case Configuration:
		return "Configuration"
	case InsertShards:
		return "InsertShards"
	case DeleteShards:
		return "DeleteShards"
	case EmptyShards:
		return "EmptyShards"
	default:
		panic(fmt.Sprintf("Unknown CommandType: %d", commandType))
	}
}

// 操作类型
type OperationType uint8

const (
	Get OperationType = iota
	Put
	Append
)

func (op OperationType) String() string {
	switch op {
	case Get:
		return "Get"
	case Put:
		return "Put"
	case Append:
		return "Append"
	default:
		panic(fmt.Sprintf("Unknown OperationType: %d", op))
	}
}

type CommandArgs struct {
	Key       string
	Value     string
	Op        OperationType
	ClientId  int64
	CommandId int64
}

func (args *CommandArgs) String() string {
	return fmt.Sprintf("CommandArgs{Key: %s, Value: %s, Op: %s, ClientId: %d, CommandId: %d}", args.Key, args.Value, args.Op, args.ClientId, args.CommandId)
}

type CommandReply struct {
	Err   Err
	Value string
}

func (reply *CommandReply) String() string {
	return fmt.Sprintf("CommandReply{Err: %s, Value: %s}", reply.Err, reply.Value)
}

type OperationContext struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

func (operationContext OperationContext) String() string {
	return fmt.Sprintf("OperationContext{MaxAppliedCommandId: %d, LastReply: %v}", operationContext.MaxAppliedCommandId, operationContext.LastReply)
}

// deepCopy creates a copy of OperationContext
func (operationContext OperationContext) deepCopy() OperationContext {
	return OperationContext{
		MaxAppliedCommandId: operationContext.MaxAppliedCommandId,
		LastReply: &CommandReply{
			Err:   operationContext.LastReply.Err,
			Value: operationContext.LastReply.Value},
	}
}

type ShardOperationArgs struct {
	ConfigNum int
	ShardIDs  []int
}

func (args *ShardOperationArgs) String() string {
	return fmt.Sprintf("ShardOperationArgs{ConfigNum: %d, ShardIDs: %v}", args.ConfigNum, args.ShardIDs)
}

type ShardOperationReply struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]OperationContext
}

func (reply *ShardOperationReply) String() string {
	return fmt.Sprintf("ShardOperationReply{Err: %s, ConfigNum: %d, Shards: %v, LastOperations: %v}", reply.Err, reply.ConfigNum, reply.Shards, reply.LastOperations)
}

type Command struct {
	CommandType CommandType
	Data        interface{}
}

func (command Command) String() string {
	return fmt.Sprintf("Command{commandType: %s, Data: %v}", command.CommandType, command.Data)
}

func NewOperationCommand(args *CommandArgs) Command {
	return Command{Operation, *args}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(reply *ShardOperationReply) Command {
	return Command{InsertShards, *reply}
}

func NewDeleteShardsCommand(args *ShardOperationArgs) Command {
	return Command{DeleteShards, *args}
}

func NewEmptyShardsCommand() Command {
	return Command{EmptyShards, nil}
}
