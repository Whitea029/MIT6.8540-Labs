package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID  int64 // client unique id
	RequestID int64 // request unique id
}

type PutAppendReply struct {
	Value string // old value
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID  int64 // client unique id
	RequestID int64 // request unique id
}

type GetReply struct {
	Value string
}
