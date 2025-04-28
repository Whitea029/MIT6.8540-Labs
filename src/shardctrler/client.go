package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	leaderId  int64
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:   servers,
		leaderId:  0,
		clientId:  nrand(),
		commandId: 0,
	}
}

func (ck *Clerk) Query(num int) Config {
	return ck.Command(&CommandArgs{
		Op:  OpQuery,
		Num: num,
	})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Command(&CommandArgs{
		Op:      OpJoin,
		Servers: servers,
	})
}

func (ck *Clerk) Leave(gids []int) {
	ck.Command(&CommandArgs{
		Op:   OpLeave,
		GIDs: gids,
	})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Command(&CommandArgs{
		Op:    OpMove,
		Shard: shard,
		GID:   gid,
	})
}

func (ck *Clerk) Command(args *CommandArgs) Config {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		reply := new(CommandReply)
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Command", args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			// 失败，换一个
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		} else {
			ck.commandId++
			return reply.Config
		}
	}
}
