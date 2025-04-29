package shardkv

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type ShardKV struct {
	mu             sync.RWMutex                   // 读写锁 提高性能
	dead           int32                          // set by Kill()
	me             int                            // id
	rf             *raft.Raft                     // raft 实例
	applyCh        chan raft.ApplyMsg             // 用于 raft apply 的 channel
	make_end       func(string) *labrpc.ClientEnd // 用于建造一个 client 与别的 group 交流
	gid            int                            // group id of the server
	sc             *shardctrler.Clerk             // 与 shardctrler 交流的 client
	maxraftstate   int                            // snapshot if log grows this big, 用来记录是否去要sanpshhot
	lastApplied    int                            // 最后应用的日志条目的索引，以防止 stateMachine 回滚
	lastConfig     shardctrler.Config             // 上一次从 shardctrler 收到的 config
	currentConfig  shardctrler.Config             // 当前集群的 config
	stateMachine   map[int]*Shard                 // KV 状态机
	lastOperations map[int64]OperationContext     // 上一次操作，用于防止网络环境不佳下重复请求
	notifyChans    map[int]chan *CommandReply     // 通知 client 获取 reply 的 chan
}

// 执行 kv client rpc Command
func (kv *ShardKV) Command(args *CommandArgs, reply *CommandReply) {
	kv.mu.RLock()
	// 检查重复请求
	if args.Op != Get && kv.isDuplicateRequest(args.ClientId, args.CommandId) {
		// 如果且为 Get 且重复请求，返回上一次请求返回内容
		lastReply := kv.lastOperations[args.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}

	// 检查当前 shard 是否可用
	if !kv.canServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}

	kv.mu.RUnlock()
	// 执行 operation command
	kv.Execute(NewOperationCommand(args), reply)
}

// kv server 执行 command
func (kv *ShardKV) Execute(command Command, reply *CommandReply) {
	// 不持有锁去提升吞吐量
	// 当 KVServer 持有锁时去存入快照，底层 raft 仍然可以提交 raft 日志

	// 尝试提交到 raft
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 获取一个 notifyChan
	kv.mu.Lock()
	notifyChan := kv.getNotifyChan(index)
	kv.mu.Unlock()

	// 等待 notifyChan 返回 或 返回超时
	select {
	case result := <-notifyChan:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	// 异步关闭这个 notifyChan
	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// 新 leader 去旧 leader 拉 shard 的数据
func (kv *ShardKV) GetShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// 只有 leader 才能获取 shard 数据，不是 leader 立刻返回
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 加读锁，只是拷贝数据，不是修改数据
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	// 当前 server 的配置还小，还没有更新到位
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// 准备把请求的 shard 拷贝回去
	// 深拷贝
	reply.Shards = make(map[int]map[string]string)
	for _, shardID := range args.ShardIDs {
		reply.Shards[shardID] = kv.stateMachine[shardID].deepCopy()
	}

	// 把每一个 client 的 lastOperation 也拷贝过来，保证幂等姓，防止迁移之后重复的请求造成重复写入
	reply.LastOperations = make(map[int64]OperationContext)
	for clientId, operationContext := range kv.lastOperations {
		reply.LastOperations[clientId] = operationContext.deepCopy()
	}

	// 成功返回
	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

// 新 leader拉到数据后，告诉旧 leader可以删掉了
func (kv *ShardKV) DeleteShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	// 只有 leader 才能获取 shard 数据，不是 leader 立刻返回
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	// 当前 server 的配置还小，还没有更新到位
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()

	// 发送请求
	commandReply := new(CommandReply)
	kv.Execute(NewDeleteShardsCommand(args), commandReply)
	reply.Err = commandReply.Err
}

// 监听 raft apply request
func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				// 如果为 command
				kv.mu.Lock()
				// 检查是否为重复 command
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				// 更新 lastApplied
				kv.lastApplied = message.CommandIndex

				reply := new(CommandReply)
				// 类型断言为 Command
				command := message.Command.(Command)
				// 判断 command 类型 提交到状态机 获取返回结果
				switch command.CommandType {
				case Operation:
					// 断言为 Operation CommandArgs 并执行操作
					operation := command.Data.(CommandArgs)
					reply = kv.applyOperation(&operation)
				case Configuration:
					// 断言为 配置操作 并提交到状态机
					nextConfig := command.Data.(shardctrler.Config)
					reply = kv.applyConfiguration(&nextConfig)
				case InsertShards:
					// 断言为 InsertShard 的 ShardOperationReply
					shardsInfo := command.Data.(ShardOperationReply)
					reply = kv.applyInsertShards(&shardsInfo)
				case DeleteShards:
					// 断言为 DeleteShard 的 ShardOperationArgs
					shardsInfo := command.Data.(ShardOperationArgs)
					reply = kv.applyDeleteShards(&shardsInfo)
				case EmptyShards:
					// 提交 EmptyShard 到状态机
					reply = kv.applyEmptyShards()
				}

				// 如果当前为 leader 且 term 合法，通知到 notifyChan
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					notifyChan := kv.getNotifyChan(message.CommandIndex)
					notifyChan <- reply
				}

				// 判断是否存入快照
				if kv.needSnapshot() {
					// 存入快照
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()

			} else if message.SnapshotValid {
				// 如果为 snapshot 恢复请求
				kv.mu.Lock()
				// 从快照中恢复状态机
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("{Node %v}{Group %v} invalid apply message %v", kv.me, kv.gid, message))
			}
		}
	}
}

// 处理 Get/Put/Append 客户端请求 到 KV状态机
func (kv *ShardKV) applyOperation(operation *CommandArgs) *CommandReply {
	reply := new(CommandReply)
	shardID := key2shard(operation.Key)

	// 判断 shard 是否可用
	if !kv.canServe(shardID) {
		reply.Err = ErrWrongGroup
	} else {
		// 判断请求是否重复
		if operation.Op != Get && kv.isDuplicateRequest(operation.ClientId, operation.CommandId) {
			// 重复则返回上次返回的内容
			lastReply := kv.lastOperations[operation.ClientId].LastReply
			reply.Value, reply.Err = lastReply.Value, lastReply.Err
		} else {
			// 提交 operation 操作到 KV状态机
			reply = kv.applyLogToStateMachine(operation, shardID)
			if operation.Op != Get {
				// 不为 GET 时更新 lastOpperation
				kv.lastOperations[operation.ClientId] = OperationContext{
					operation.CommandId,
					reply,
				}
			}
		}
	}
	// 返回结果
	return reply
}

// 应用一个新的 shard 配置（来自 shard controller 的 reconfiguration，比如迁移 shard）
func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *CommandReply {
	reply := new(CommandReply)
	// 判断 config num 是否为当前 +1，防止错乱情况
	if nextConfig.Num == kv.currentConfig.Num+1 {
		// 基于新的 config 更新当前状态
		kv.updateShardStatus(nextConfig)
		// 保存上一份 config
		kv.lastConfig = kv.currentConfig
		// 更新当前 config
		kv.currentConfig = *nextConfig
		reply.Err = OK
	} else {
		reply.Err = ErrOutDated
	}
	// 返回结果
	return reply
}

// 应用迁移过来的 shard 数据（插入 shard）
func (kv *ShardKV) applyInsertShards(shardsInfo *ShardOperationReply) *CommandReply {
	reply := new(CommandReply)
	// 先检查 ConfigNum 和当前的一样，不一样就丢弃（防止老数据污染新集群）
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for shardID, shardData := range shardsInfo.Shards {
			shard := kv.stateMachine[shardID]
			if shard.Status == Pulling {
				// 必须是 Pulling 状态才能插入数据
				for key, value := range shardData {
					shard.Put(key, value)
				}
				// 插入完，把状态改为 GCing
				shard.Status = GCing
			} else {
				break
			}
		}

		// 最后还要更新一下 lastOperations
		for clientId, operationContext := range shardsInfo.LastOperations {
			if lastOperation, ok := kv.lastOperations[clientId]; !ok || lastOperation.MaxAppliedCommandId < operationContext.MaxAppliedCommandId {
				kv.lastOperations[clientId] = operationContext
			}
		}
	} else {
		// 否则过时
		reply.Err = ErrOutDated
	}
	// 返回结果
	return reply
}

// 删除 shard
func (kv *ShardKV) applyDeleteShards(shardsInfo *ShardOperationArgs) *CommandReply {
	// 也是先检查 ConfigNum 是否匹配
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardID := range shardsInfo.ShardIDs {
			// 拿到要删除的 shard
			shard := kv.stateMachine[shardID]
			if shard.Status == GCing {
				// 如果状态是 GCing，说明数据已经拿到手了，可以改成 Serving，开始正常对外提供服务
				shard.Status = Serving
			} else if shard.Status == BePulling { // if the shard is being pulled, reset the shard to a new one
				// 如果状态是 BePulling，说明自己是被别人拉走了的 shard，直接重置为 NewShard（清空）
				kv.stateMachine[shardID] = NewShard()
			} else {
				// 其他情况说明出现重复删除或者不合理删除
				break
			}
		}
		return &CommandReply{Err: OK}
	}
	// ConfigNum 不匹配也直接返回
	return &CommandReply{Err: OK}
}

func (kv *ShardKV) applyEmptyShards() *CommandReply {
	return &CommandReply{Err: OK}
}

// 通过新的 config 更新当前 shard 的状态的
func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	for shardID := 0; shardID < shardctrler.NShards; shardID++ {
		// 判断当前 shard 是否不归属于当前 gid 而下一个 shard 归属于当前 gid
		// 该 shard 不是这个 gid 负责的，但是接下来配置中的 gid 是负责这个 shard 的，所以需要拉取该 shard
		if kv.currentConfig.Shards[shardID] != kv.gid && nextConfig.Shards[shardID] == kv.gid {
			// 拿到新的 gid
			gid := kv.currentConfig.Shards[shardID]
			// 跳过 gid 0
			if gid != 0 {
				kv.stateMachine[shardID].Status = Pulling
			}
		}
	}
}

// 提交 operation 操作到 KV状态机
func (kv *ShardKV) applyLogToStateMachine(operation *CommandArgs, shardID int) *CommandReply {
	reply := new(CommandReply)
	switch operation.Op {
	case Get:
		reply.Value, reply.Err = kv.stateMachine[shardID].Get(operation.Key)
	case Put:
		reply.Err = kv.stateMachine[shardID].Put(operation.Key, operation.Value)
	case Append:
		reply.Err = kv.stateMachine[shardID].Append(operation.Key, operation.Value)
	}
	return reply
}

// 判断是否需要存入快照
func (kv *ShardKV) needSnapshot() bool {
	// 不为 -1 且 当前 size 大于 maxraftstate
	return kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

// 存入快照
func (kv *ShardKV) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastConfig)     // 上一次 config
	e.Encode(kv.currentConfig)  // 当前 config
	e.Encode(kv.stateMachine)   // 状态机
	e.Encode(kv.lastOperations) // 上一次操作
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

// 通过快照重建 KV状态机
func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	// 如果没有快照就初始化状态机
	if snapshot == nil || len(snapshot) < 1 {
		kv.initStateMachines()
		return
	}
	// 重建状态机
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine map[int]*Shard
	var lastOperations map[int64]OperationContext
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config
	if d.Decode(&lastConfig) != nil ||
		d.Decode(&currentConfig) != nil ||
		d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperations) != nil {
		DPrintf("{Node %v}{Group %v} fails to restore state machine from snapshot", kv.me, kv.gid)
	}
	kv.lastConfig, kv.currentConfig, kv.stateMachine, kv.lastOperations = lastConfig, currentConfig, stateMachine, lastOperations
}

// 初始化 KV状态机
func (kv *ShardKV) initStateMachines() {
	for shardID := 0; shardID < shardctrler.NShards; shardID++ {
		if _, ok := kv.stateMachine[shardID]; !ok {
			kv.stateMachine[shardID] = NewShard()
		}
	}
}

// 用于检查请求是否重复
func (kv *ShardKV) isDuplicateRequest(clientId int64, commandId int64) bool {
	operationContext, ok := kv.lastOperations[clientId]
	// 存在且请求 id 小于最大请求 id
	return ok && commandId <= operationContext.MaxAppliedCommandId
}

// 判断当前 server 是否可以使用这个 shard
func (kv *ShardKV) canServe(shardID int) bool {
	// shard 要归属于当前 server 的 gid
	// shard 要处于 serving or gcing 状态
	return kv.currentConfig.Shards[shardID] == kv.gid && (kv.stateMachine[shardID].Status == Serving || kv.stateMachine[shardID].Status == GCing)
}

// 获取一个 notifyChan
func (kv *ShardKV) getNotifyChan(index int) chan *CommandReply {
	if _, ok := kv.notifyChans[index]; !ok {
		// 不存在就创建一个
		kv.notifyChans[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChans[index]
}

// 释放一个 notifyChan
func (kv *ShardKV) removeOutdatedNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

// 负责检测是否可以应用新的配置（比如分片重分配）
func (kv *ShardKV) configurationAction() {
	canPerformNextConfig := true
	// 加锁
	kv.mu.RLock()
	// 遍历所有状态机
	// 如果任何分片不是 Serving 状态，说明还有迁移没完成，就不能切换到下一轮配置
	for _, shard := range kv.stateMachine {
		if shard.Status != Serving {
			canPerformNextConfig = false
			break
		}
	}
	// 记录当前配置num
	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()
	// 如果全部是 Serving
	if canPerformNextConfig {
		// 查询下一个 num 的配置
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		// 确保查询到的确实是下一个 num 的配置 （避免在迁移未完成时，错误地推进到新一轮配置）
		// TODO:
		if nextConfig.Num == currentConfigNum+1 {
			// 执行配置迁移
			kv.Execute(NewConfigurationCommand(&nextConfig), new(CommandReply))
		}
	}
}

// 负责拉取（Pull）需要迁移过来的分片数据
func (kv *ShardKV) migrationAction() {
	kv.mu.Lock()
	// 先查找所有本地处于 Pulling 状态的分片
	gid2Shards := kv.getShardIDsByStatus(Pulling)

	var wg sync.WaitGroup
	for gid, shardIds := range gid2Shards {
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTasksArgs := ShardOperationArgs{configNum, shardIDs}
			for _, server := range servers {
				pullTasksReply := &ShardOperationReply{}
				s := kv.make_end(server)
				if s.Call("ShardKV.GetShardsData", &pullTasksArgs, pullTasksReply) && pullTasksReply.Err == OK {
					// 把拉到的 shard 数据插入到自己本地状态机里
					kv.Execute(NewInsertShardsCommand(pullTasksReply), new(CommandReply))
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}

	kv.mu.RUnlock()
	// 使用 WaitGroup 等所有拉取任务完成再返回，并行拉取提高效率
	wg.Wait()
}

// 执行垃圾回收动作
func (kv *ShardKV) gcAction() {
	kv.mu.RLock()
	gid2Shards := kv.getShardIDsByStatus(Pulling)

	var wg sync.WaitGroup
	for gid, shardIds := range gid2Shards {
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			gcTasksArgs := ShardOperationArgs{configNum, shardIDs}
			for _, server := range servers {
				gcTaskReply := &ShardOperationReply{}
				s := kv.make_end(server)
				if s.Call("ShardKV.DeleteShardsData", &gcTasksArgs, gcTaskReply) && gcTaskReply.Err == OK {
					// 把 GC 的 shards 标记掉（或者真正删除）
					kv.Execute(NewDeleteShardsCommand(&gcTasksArgs), new(CommandReply))
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}

	kv.mu.RUnlock()
	// 使用 WaitGroup 等所有拉取任务完成再返回，并行拉取提高效率
	wg.Wait()
}

// 向 Raft 提交一个空命令，防止日志停滞不前，保障领导者身份正常保持
func (kv *ShardKV) checkEntryInCurrentTermAction() {
	// If no log entry exists in the current term, execute an empty command
	if !kv.rf.HasLogInCurrentTerm() {
		kv.Execute(NewEmptyShardsCommand(), new(CommandReply))
	}
}

// leader 通用的监控器
// for 无限循环
func (kv *ShardKV) Monitor(action func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

// 获取特定状态的 shard 的 shardID
func (kv *ShardKV) getShardIDsByStatus(status ShardStatus) map[int][]int {
	// k: GID v: shardIds
	gid2shardIds := make(map[int][]int, 0)
	for shardId, shard := range kv.stateMachine {
		if shard.Status == status {
			// 过滤出指定 status 的 shard
			gid := kv.lastConfig.Shards[shardId]
			if gid != 0 {
				if _, ok := gid2shardIds[gid]; !ok {
					gid2shardIds[gid] = make([]int, 0)
				}
				gid2shardIds[gid] = append(gid2shardIds[gid], shardId)
			}
		}
	}
	return gid2shardIds
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(CommandArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationArgs{})
	labgob.Register(ShardOperationReply{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV{
		dead:           0,
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		make_end:       make_end,
		gid:            gid,
		sc:             shardctrler.MakeClerk(ctrlers),
		maxraftstate:   maxraftstate,
		lastApplied:    0,
		lastConfig:     shardctrler.DefaultConfig(),
		currentConfig:  shardctrler.DefaultConfig(),
		stateMachine:   make(map[int]*Shard),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandReply),
	}

	kv.restoreSnapshot(persister.ReadSnapshot())

	// 监听 raft apply request
	go kv.applier()

	// 监听一些行为
	go kv.Monitor(kv.configurationAction, ConfigurationMonitorTimeout)         // Monitor configuration changes.
	go kv.Monitor(kv.migrationAction, MigrationMonitorTimeout)                 // Monitor shard migration.
	go kv.Monitor(kv.gcAction, GCMonitorTimeout)                               // Monitor garbage collection of old shard data.
	go kv.Monitor(kv.checkEntryInCurrentTermAction, EmptyEntryDetectorTimeout) // Monitor Raft log entries in the current term.

	return kv
}
