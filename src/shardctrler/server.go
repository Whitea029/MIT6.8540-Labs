package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	stateMachine  ConfigStateMachine
	lastOperation map[int64]OpContext
	notifyChs     map[int]chan *CommandReply

	// configs []Config // indexed by config num
}

func (sc *ShardCtrler) Command(args *CommandArgs, reply CommandReply) {
	sc.mu.RLock()
	if args.Op != OpQuery && sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		// 重复请求， 返回上一次返回的就可以
		LastReply := sc.lastOperation[args.ClientId].LastReply
		reply.Config, reply.Err = LastReply.Config, LastReply.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	// 尝试提交到 raft
	index, _, isLeader := sc.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 获取一个 notifyChan 等待结果
	sc.mu.Lock()
	notifyChan := sc.getNotifyChan(index)
	sc.mu.Unlock()

	// 等到结果就返回，超时就返回错误
	select {
	case result := <-notifyChan:
		reply.Config, reply.Err = result.Config, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	// 异步删除这个 notifyChan
	go func() {
		sc.mu.Lock()
		sc.removeNotifyChan(index)
		sc.mu.Unlock()
	}()
}

// 判断是否是重复请求
func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	// 存在且 commandId 小于 最大提交的
	OperationContext, ok := sc.lastOperation[clientId]
	return ok && commandId <= OperationContext.MaxAppliedCommandId
}

// 获取指定的 notifyChan 如果没有就新建一个
func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandReply {
	notifyChan, ok := sc.notifyChs[index]
	if !ok {
		notifyChan = make(chan *CommandReply, 1)
		sc.notifyChs[index] = notifyChan
	}
	return notifyChan
}

// 移除指定的 notifyChan
func (sc *ShardCtrler) removeNotifyChan(index int) {
	delete(sc.notifyChs, index)
}

// 监听 raft 的 apply 请求并发送到 notifyChs
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case message := <-sc.applyCh:
			if message.CommandValid {
				reply := new(CommandReply)
				command := message.Command.(Command)

				sc.mu.Lock()
				if command.Op != OpQuery && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					reply = sc.lastOperation[command.ClientId].LastReply
				} else {
					// 提交到状态机
					reply = sc.applyLog2StateMachine(command)
					if command.Op != OpQuery {
						// 如果不是 query 就要更行 lastOperation
						sc.lastOperation[command.ClientId] = OpContext{
							MaxAppliedCommandId: command.CommandId,
							LastReply:           reply,
						}
					}
				}

				// 确保当前 term 和 state 为 leader，通知到 notifyChan
				if currentTerm, isLeader := sc.rf.GetState(); currentTerm == message.CommandTerm && isLeader {
					notifyChan := sc.getNotifyChan(message.CommandIndex)
					notifyChan <- reply
				}
				sc.mu.Unlock()
			}
			// 不需要 SnapShot
		}
	}
}

// 提交到状态机
func (sc *ShardCtrler) applyLog2StateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case OpJoin:
		reply.Err = sc.stateMachine.Join(command.Servers)
	case OpLeave:
		reply.Err = sc.stateMachine.Leave(command.GIDs)
	case OpMove:
		reply.Err = sc.stateMachine.Move(command.Shard, command.GID)
	case OpQuery:
		reply.Config, reply.Err = sc.stateMachine.Query(command.Num)
	}
	return reply
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})
	apply := make(chan raft.ApplyMsg)

	sc := &ShardCtrler{
		rf:            raft.Make(servers, me, persister, apply),
		me:            me,
		applyCh:       apply,
		lastOperation: make(map[int64]OpContext),
		notifyChs:     make(map[int]chan *CommandReply),
		dead:          0,
	}

	// 启动监听 apply request
	go sc.applier()
	return sc
}
