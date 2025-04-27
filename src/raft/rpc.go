package raft

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果对方的任期 args.Term 比自己小，直接拒绝
	// 或者，即使任期一样，但是自己已经投过票，而且投给的不是他，也拒绝
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// 如果对方 term 比自己大，更新自己
	if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	// 检查日志新旧（超关键，防止脑裂）
	// 只有当 candidate 的日志比自己更新，或者一样新，才能投票给他
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// 投票
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

// 心跳 and 日志同步
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果对方（Leader）的 term 小于自己，说明 Leader 过时了
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// 自己落后了，立刻同步 term
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	// 立刻切换到 Follower，并重置选举超时器
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// 如果本地已经 snapshot 了，并且 snapshot 的 index 比 PrevLogIndex 大，拒绝
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// check the log is matched, if not, return the conflict index and term
	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it(§5.3)
	// 如果 PrevLogIndex 的 term 和本地的 term 不一致，拒绝（日志冲突）
	// 同时返回 ConflictIndex 和 ConflictTerm，帮助 Leader 快速调整 nextIndex
	if !rf.isLogMatched(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastLogIndex := rf.getLastLog().Index
		// 找到第一条冲突的日志
		if lastLogIndex < args.PrevLogIndex {
			// 最后一个日志索引小于 prevLogIndex，则冲突索引为最后一个日志索引
			reply.ConflictIndex, reply.ConflictTerm = lastLogIndex+1, -1
		} else {
			firstLogIndex := rf.getFirstLog().Index
			index := args.PrevLogIndex
			for index >= firstLogIndex && rf.logs[index-firstLogIndex].Term == args.PrevLogTerm {
				index--
			}
			reply.ConflictIndex, reply.ConflictTerm = index+1, args.PrevLogTerm
		}
		return
	}
	// 如果检查通过了，追加新日志 entries
	firstLogIndex := rf.getFirstLog().Index
	for index, entry := range args.Entries {
		// 找到第一个冲突点，如果本地日志和 Leader 的日志不一样，裁掉本地后面的日志。
		// 然后 append 上 Leader 给的新 entries
		if entry.Index-firstLogIndex >= len(rf.logs) || rf.logs[entry.Index-firstLogIndex].Term != entry.Term {
			rf.logs = shrinkEntries(append(rf.logs[:entry.Index-firstLogIndex], args.Entries[index:]...))
			rf.persist()
			break
		}
	}

	// 如果 Leader 的 LeaderCommit 比自己大，把自己 commit 到最新的位置。
	// 但是最多只能 commit 到自己现有日志的最后一条
	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// check the snapshot is more up-to-date than the current log
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}
