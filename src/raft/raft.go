package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// 换成 RWMutex 提升性能
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state          NodeState     // currentState NodeState
	electionTimer  *time.Timer   // timer for leader election
	heartbeatTimer *time.Timer   // timer for heartbeat
	applyCh        chan ApplyMsg // channel to send apply msg to services
	// cond for apply Groutine
	// 用于监控日志状态，一旦日志提交，负责将这些条目复制到状态机
	applyCond *sync.Cond
	// cond for replicat Groutine
	// 要与出自己以外的所有服务器进行日志同步
	replicatorCond []*sync.Cond

	// persistent state
	// 用于记录当前服务器的数据状态
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile state
	// 用于记录当前服务器的数据状态
	commitIndex int
	lastApplied int

	// Volatile state
	// 用于记录集群内每个服务器的状态，当前服务器为Leader时使用
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) ChangeState(state NodeState) {
	if rf.state == state {
		return
	}
	DPrintf("Server %d change state to %v\n", rf.me, state)
	rf.state = state
	switch state {
	// Follower 状态下，重置选举超时
	case Follower:
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.heartbeatTimer.Stop()
	// Candidate 状态下，超时重置逻辑在StartElection()中
	case Candidate:
	// Leader 状态下，重置心跳超时
	case Leader:
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeState(), nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("{Node %v} fails to decode persisted state", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
	rf.lastApplied, rf.commitIndex = rf.getFirstLog().Index, rf.getFirstLog().Index
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// 本地安装快照，裁剪掉已经包含在快照中的日志，减少内存占用，同时持久化状态
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex || index > rf.getLastLog().Index {
		// 要快照的 index 太旧了，已经被快照过了 or index 太新了，日志都还没跟上，不能快照
		return
	}
	// remove log entries up to index
	rf.logs = shrinkEntries(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil // 防止快照截断导致丢失 Term/Index 信息
	rf.persister.Save(rf.encodeState(), snapshot)
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects outdated snapshot with lastIncludeIndex %v as current commitIndex %v is larger in term %v", rf.me, lastIncludedIndex, rf.commitIndex, rf.currentTerm)
		return false
	}
	// need dummy entry at index 0
	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = shrinkEntries(rf.logs[lastIncludedIndex-rf.getFirstLog().Index:])
		rf.logs[0].Command = nil
	}
	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex
	rf.persister.Save(rf.encodeState(), snapshot)

	return true
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	// first append log entry for itself
	newLogIndex := rf.getLastLog().Index + 1
	rf.logs = append(rf.logs, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   newLogIndex,
	})
	rf.persist()
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLogIndex, newLogIndex+1
	// then broadcast to all peers to append log entry
	rf.BroadcastHeartbeat(false)
	// return the new log index and term, and whether this server is the leader
	return newLogIndex, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) advanceCommitIndexForLeader() {
	// 计算半数节点的 commitIndex
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		// 如果半数节点已经提交的日志，而且这条日志也是当前 Term 产生的
		if rf.isLogMatched(newCommitIndex, rf.currentTerm) {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}

func (rf *Raft) replicateOnceRound(peer int) {
	rf.mu.RLock()
	// 如果是 leader 不处理
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// follower 日志过于老了，leader 已经没有这部分日志，只能同步 snapsho太
		// 只需要发送 InstallSnapshot
		args := rf.genInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := &InstallSnapshotReply{}
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			// 保证状态不变
			if rf.checkContext(args.Term, Leader) {
				if reply.Term > rf.currentTerm {
					// 如果当前Term比自己的大，说明自己已经过期了
					rf.ChangeState(Follower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
				} else {
					rf.nextIndex[peer] = args.LastIncludedIndex + 1
					rf.matchIndex[peer] = args.LastIncludedIndex
				}
			}
			rf.mu.Unlock()
		}
	} else {
		// 发送 AppendEntries
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.RUnlock()
		reply := &AppendEntriesReply{}
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			if rf.checkContext(args.Term, Leader) {
				if !reply.Success {
					// 处理冲突
					if reply.Term > rf.currentTerm {
						// 如果当前Term比自己的大，说明自己已经过期了
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					} else if reply.Term == rf.currentTerm {
						// 日志冲突，减小 nextIndex 然后 retry
						rf.nextIndex[peer] = reply.ConflictIndex
						if reply.ConflictTerm != -1 {
							// 如果知道冲突的 term，可以二分查找最后一个 ConflictTerm 出现的位置，加速回退
							// 二分查找
							firstLogIndex := rf.getFirstLog().Index
							low, hight := firstLogIndex, args.PrevLogIndex-1
							for low < hight {
								mid := (low + hight + 1) / 2
								if rf.logs[mid-firstLogIndex].Term <= reply.ConflictTerm {
									low = mid
								} else {
									hight = mid - 1
								}
							}
							if rf.logs[low-firstLogIndex].Term == reply.ConflictTerm {
								rf.nextIndex[peer] = low
							}
						}
					}
				} else {
					// 日志复制成功
					rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) // 更新到 最新复制成功的日志下标
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1                // 指向下次要发的日志
					// 尝试推进 commitIndex（比如大多数副本都复制到某个位置了，可以提交了）
					rf.advanceCommitIndexForLeader()
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// 发送心跳
			go rf.replicateOnceRound(peer)
		} else {
			// 唤醒 peer relicator groutine
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) StartElection() {
	rf.votedFor = rf.me
	rf.persist() // 持久化
	args := rf.genRequestVoteArgs()
	votes := 1

	requestVote2Peers := func(peer int) {
		reply := &RequestVoteReply{}
		if rf.sendRequestVote(peer, args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 保证状态不变时
			if rf.checkContext(args.Term, Candidate) {
				if reply.VoteGranted {
					votes++
					// 选票数达到一半，成为Leader
					if votes > len(rf.peers)/2 {
						rf.ChangeState(Leader)
						rf.BroadcastHeartbeat(true)
					}
				} else if reply.Term > rf.currentTerm {
					// 如果收到的回复的Term比自己的大，说明自己已经过期了
					rf.ChangeState(Follower)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist() // 持久化
				}
			}
		}
	}

	for peer := range rf.peers {
		if peer != rf.me {
			go requestVote2Peers(peer)
		}
	}

}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// 检查日志新旧
func (rf *Raft) isLogUpToDate(index, term int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

// 检查状态是否改变
func (rf *Raft) checkContext(term int, state NodeState) bool {
	return rf.currentTerm == term && rf.state == state
}

// 日志是否匹配
func (rf *Raft) isLogMatched(index, term int) bool {
	return index <= rf.getLastLog().Index && term == rf.logs[index-rf.getFirstLog().Index].Term
}

// 判断这个 peer 是否需要同步日志
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// 自己是 Leader 且 follower 的 matchIndex（已经同步完成的日志index）落后于 leader 当前最新日志的 index
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

// 这是每个 follower 单独的同步线程。
// 它在后台循环干活，每次等别人叫醒自己，然后同步日志
func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		// 看是否需要同步日志
		for !rf.needReplicating(peer) {
			// 不用的话就 Wait
			rf.replicatorCond[peer].Wait()
		}
		// send log entries to peer
		rf.replicateOnceRound(peer)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		// 超时未收到心跳，开始选举
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm++
			rf.persist()       // 持久化
			rf.StartElection() // 开始选举
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.mu.Unlock()
		// 定期发送心跳爆
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			// 只有Leader才会发送心跳
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

// 把已经 commit 的日志，真正 apply 到状态机（通过 applyCh 发出去）
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			// 如果还没有新 commit 的日志，就睡觉等 applyCond
			rf.applyCond.Wait()
		}
		// apply log to state machine
		firstLogIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		// 注意 unlock send applyCh 是比较慢的 IO 操作，要在外面做，不要一直持锁，避免卡住其他线程
		rf.mu.Unlock()
		// send the apply message to applyCh for service/State Machine Replica
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		// 修改 lastApplied 是 shared 状态，要加锁保护
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:             sync.RWMutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]LogEntry, 1), // dummy entry at index 0
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		state:          Follower,
		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 初始化nextIndex 和 matchIndex，开启 replicator goroutine
	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastLog().Index+1
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			// 启动 replicator goroutine
			go rf.replicator(peer)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	// 启动 apply goroutine 将日志条目应用到状态机
	go rf.applier()

	return rf
}
