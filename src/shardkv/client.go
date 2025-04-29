package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int { // 通过 key 拿到 shard
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	// key: gid val: leaderid
	leaderIds map[int]int
	commandId int64
	clientId  int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		make_end:  make_end,
		leaderIds: make(map[int]int),
		clientId:  nrand(),
		commandId: 0,
	}
	// 尝试获取最新的 config
	ck.config = ck.sm.Query(-1)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
// Get is a wrapper function for Command
func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandArgs{Key: key, Op: Get})
}

// Put is a wrapper function for Command
func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: Put})
}

// Append is a wrapper function for Command
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: Append})
}

// 执行命令
func (ck *Clerk) Command(args *CommandArgs) string {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		// 拿到 shard 和 gid
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// 如果没有设置 leaderId 先设置成 0
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			oldLeaderId := ck.leaderIds[gid]
			newLeader := oldLeaderId
			for {
				reply := new(CommandReply)
				// send the request to the leader server
				ok := ck.make_end(servers[newLeader]).Call("ShardKV.Command", args, reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.commandId++
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					// 重试
					break
				} else {
					// 切换 leader 重试
					newLeader = (newLeader + 1) % len(servers)
					// check if all servers have been tried
					if newLeader == oldLeaderId {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// 尝试获取最新的配置
		ck.config = ck.sm.Query(-1)
	}
}
