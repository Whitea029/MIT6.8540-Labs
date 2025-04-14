package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	store        map[string]string // key-value store
	lastApplied  map[int64]int64   // last applied request id for each client key: clientID value: requestID
	lastResponse map[int64]string  // last response for each client key: clientID value: old value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// for Get reuqest, we don't need to check repeat request
	value, ok := kv.store[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// for Put request, we need to check repeat request
	lastRequestID, ok := kv.lastApplied[args.ClientID]
	if ok && lastRequestID >= args.RequestID {
		return
	}
	kv.store[args.Key] = args.Value
	// record the last response
	kv.lastApplied[args.ClientID] = args.RequestID
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// for Append request, we need to check repeat request
	lastRequestID, ok := kv.lastApplied[args.ClientID]
	if ok && lastRequestID >= args.RequestID {
		if args.RequestID == lastRequestID {
			reply.Value = kv.lastResponse[args.ClientID]
			return
		}
	}

	value := kv.store[args.Key]
	reply.Value = value
	kv.store[args.Key] = value + args.Value
	// record the last response
	kv.lastApplied[args.ClientID] = args.RequestID
	// record the last response
	kv.lastResponse[args.ClientID] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	kv.store = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.lastResponse = make(map[int64]string)

	return kv
}
