package kvraft

import (
	"bytes"
	"fmt"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"

	"sync"
	"sync/atomic"
)

// 操作上下文，用于记录最大的已应用命令ID和上一次回复
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	lastAppliedIndex int            // last applied index
	stateMachine     KVStateMachine // state machine
	lastOperations   map[int64]Op   // last operation for each client
	notifyChs        map[int]chan *CommandReply
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 监听 raft applyCh
func (kv *KVServer) applier() {
	// for 循环
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				// 如果是操作命令
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastAppliedIndex {
					// 重复 apply 请求
					kv.mu.Unlock()
					continue
				}

				kv.lastAppliedIndex = message.CommandIndex

				reply := new(CommandReply)
				command := message.Command.(Command)
				if command.Op != OpGet && kv.isDuplicatedCommand(command.ClientId, command.CommandId) {
					// 重复命令
					reply = kv.lastOperations[command.ClientId].LastReply
				} else {
					// 提交状态机，内存存储
					reply = kv.applyLogToStateMachine(command)
					if command.Op != OpGet {
						// 如果不是 GET 更新操作记录
						kv.lastOperations[command.ClientId] = Op{
							MaxAppliedCommandId: command.CommandId,
							LastReply:           reply,
						}
					}
				}

				// 当前 rf 为 leader 通知对应的通道，返回结果
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyCh(message.CommandIndex)
					ch <- reply
				}

				// 如果超过最大 raft state 则进行快照
				if kv.needSnapshot() {
					kv.installSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
			} else if message.SnapshotValid {
				// 如果是快照命令
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					// 通过快照恢复状态机
					kv.restoreStateFromSnapshot(message.Snapshot)
					kv.lastAppliedIndex = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("Invalid ApplyMsg %v", message))
			}
		}
	}
}

func (kv *KVServer) restoreStateFromSnapshot(snapshot []byte) {
	if len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine MemoryKV
	var lastOperations map[int64]Op
	if d.Decode(&stateMachine) != nil || d.Decode(&lastOperations) != nil {
		panic("Failed to decode snapshot")
	}
	kv.stateMachine = &stateMachine
	kv.lastOperations = lastOperations
}

// 持久化快照
func (kv *KVServer) installSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperations)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

// 提交请求到状态机
func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case OpGet:
		reply.Value, reply.Err = kv.stateMachine.Get(command.Key)
	case OpPut:
		reply.Err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		reply.Err = kv.stateMachine.Append(command.Key, command.Value)
	}
	return reply
}

// 判断是否需要快照
func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

// 执行命令
func (kv *KVServer) ExecuteCommand(args *CommandArgs, reply *CommandReply) {
	// 读锁
	kv.mu.RLock()
	// 不为 GET 且 判断是否是重复命令
	if args.Op != OpGet && kv.isDuplicatedCommand(args.ClientId, args.CommandId) {
		// 重复请求，返回上一次结果
		r := kv.lastOperations[args.ClientId].LastReply
		reply.Value, reply.Err = r.Value, r.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.RUnlock()

	// 调用 raft, 提交命令
	index, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 获取通知管道
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	// 阻塞等待 raft apply 通知到 applyCh 或超时
	select {
	case r := <-ch:
		reply.Value, reply.Err = r.Value, r.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.deleteNotifyCh(index)
		kv.mu.Unlock()
	}()

}

// 删除通知通道
func (kv *KVServer) deleteNotifyCh(index int) {
	delete(kv.notifyChs, index)
}

// 判断是否是重复命令
func (kv *KVServer) isDuplicatedCommand(clientId, commandId int64) bool {
	op, ok := kv.lastOperations[clientId]
	// 存在 且 小于最大id
	return ok && commandId <= op.MaxAppliedCommandId
}

// 获取通知通道
func (kv *KVServer) getNotifyCh(index int) chan *CommandReply {
	// 如果不存在，则创建一个新的通道
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChs[index]
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})

	// 初始化
	applyCh := make(chan raft.ApplyMsg)
	kv := &KVServer{
		mu:             sync.RWMutex{},
		me:             me,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		dead:           0,
		maxraftstate:   maxraftstate,
		stateMachine:   &MemoryKV{KV: make(map[string]string)},
		lastOperations: make(map[int64]Op),
		notifyChs:      make(map[int]chan *CommandReply),
	}

	// 先尝试从快照中恢复状态机
	kv.restoreStateFromSnapshot(persister.ReadSnapshot())

	// 启动一个 goroutine 处理 applyCh
	go kv.applier()

	return kv
}
