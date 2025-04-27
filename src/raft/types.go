package raft

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type NodeState int8

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// InstallSnapshot
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// unused fields
	// Offset int	// byte offset where chunk is positioned in the snapshot file
	// Done   bool	// true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.getFirstLog().Index,
		LastIncludedTerm:  rf.getFirstLog().Term,
		Data:              rf.persister.ReadSnapshot(),
	}
}

// RequestVote
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
}

// AppendEntries
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().Index
	entries := make([]LogEntry, len(rf.logs[prevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.logs[prevLogIndex-firstLogIndex+1:])
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,                             // 前一个日志的索引
		PrevLogTerm:  rf.logs[prevLogIndex-firstLogIndex].Term, // 前一个日志的任期
		LeaderCommit: rf.commitIndex,                           // Leader的已提交索引
		Entries:      entries,
	}
}
