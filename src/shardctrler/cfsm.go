package shardctrler

import "sort"

// 配置状态机 接口
type ConfigStateMachine interface {
	Join(groups map[int][]string) Err
	Leave(gids []int) Err
	Move(shard, gid int) Err
	Query(num int) (Config, Err)
}

// 基于内存的配置状态机
type MemoryConfigStateMachine struct {
	Configs []Config
}

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	cf := &MemoryConfigStateMachine{make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (m *MemoryConfigStateMachine) Join(groups map[int][]string) Err {
	// 基于最后一个配置的基础上，创建一个新的配置
	lastConfig := m.Configs[len(m.Configs)-1]
	newConfig := Config{
		Num:    len(m.Configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups), // 深拷贝
	}

	// 存入 group
	for gid, servers := range groups {
		// 如果不存在就新建一个 group
		if _, ok := newConfig.Groups[gid]; !ok {
			// 注意要拷贝，不能是遍历，
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// 转化成 map[int][]int key 为 gid，value 为 shard 列表
	group2Shards := group2Shards(newConfig)
	// 负载均衡，尽量保证各个 group 拥有的 shard 数量相同
	for {
		// 找到拥有最多和最少 shard 的 gid
		source, target := GetGIDWithMaximumShards(group2Shards), GetGIDWithMinimumShards(group2Shards)
		if source != 0 && len(group2Shards[source])-len(group2Shards[target]) <= 1 {
			// 相同
			break
		}
		// 把一个 shard 从 source (max) 迁移到 target (min)
		group2Shards[target] = append(group2Shards[target], group2Shards[source][0])
		group2Shards[source] = group2Shards[source][1:]
	}

	// 更新
	var newShards [NShards]int
	for gid, shards := range group2Shards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	newConfig.Shards = newShards
	m.Configs = append(m.Configs, newConfig)
	return OK
}

func (m *MemoryConfigStateMachine) Leave(gids []int) Err {
	// 基于最后一个配置的基础上，创建一个新的配置
	lastConfig := m.Configs[len(m.Configs)-1]
	newConfig := Config{
		Num:    len(m.Configs),
		Shards: lastConfig.Shards,
		Groups: deepCopy(lastConfig.Groups), // 深拷贝
	}
	group2Shards := group2Shards(newConfig)

	orphanShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			// 删除 config 中注册的 gid
			delete(newConfig.Groups, gid)
		}
		// 删除 group 中的 shard
		if shards, ok := group2Shards[gid]; ok {
			delete(group2Shards, gid)
			// 添加到 孤立shard
			orphanShards = append(orphanShards, shards...)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) > 0 {
		// 如果还剩余 group
		// 先处理 孤儿shard
		for _, shard := range orphanShards {
			// 将 孤立shard 放到剩余的 group 中
			// 负载均衡，每次放都找最少 shard 的放
			gid := GetGIDWithMinimumShards(group2Shards)
			newShards[shard] = gid
			group2Shards[gid] = append(group2Shards[gid], shard)
		}

		// 然后处理所有的 shard：孤儿shard + 没有变化的 shard
		for gid, shards := range group2Shards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	// 存入
	newConfig.Shards = newShards
	m.Configs = append(m.Configs, newConfig)
	return OK
}

func (m *MemoryConfigStateMachine) Move(shard, gid int) Err {
	// 基于最后一个配置的基础上，创建一个新的配置
	lastConfig := m.Configs[len(m.Configs)-1]
	newConfig := Config{
		len(m.Configs),
		lastConfig.Shards,
		deepCopy(lastConfig.Groups),
	}

	// 存入
	newConfig.Shards[shard] = gid
	m.Configs = append(m.Configs, newConfig)
	return OK
}

func (m *MemoryConfigStateMachine) Query(num int) (Config, Err) {
	if num < 0 || num > len(m.Configs) {
		// 如果 num 不合法
		return m.Configs[len(m.Configs)-1], OK
	}
	return m.Configs[num], OK
}

// 获取 shard 最多的 gid
func GetGIDWithMaximumShards(group2Shards map[int][]int) int {
	// 表明没有分配 group
	if shards, ok := group2Shards[0]; ok && len(shards) != 0 {
		return 0
	}

	// get all the group IDs
	var gids []int
	for gid := range group2Shards {
		gids = append(gids, gid)
	}
	// 排序 保证shard数量一致时，选编号最小的 group
	// 这是一个细节，为的是保证确定性，不然每次调用可能返回不同的输出，这是不稳定的，下面方法同理
	sort.Ints(gids)
	index, maxShards := -1, -1
	// 找到最多 shard 的 gid
	for _, gid := range gids {
		if len(group2Shards[gid]) > maxShards {
			index, maxShards = gid, len(group2Shards[gid])
		}
	}
	return index
}

// 获取 shard 最少的 gid
func GetGIDWithMinimumShards(group2Shards map[int][]int) int {
	// get all the group IDs
	var gids []int
	for gid := range group2Shards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	index, minShards := -1, NShards+1
	// 找到最少 shard 的 gid
	for _, gid := range gids {
		// 不要选择 group 0
		if gid != 0 && len(group2Shards[gid]) < minShards {
			index, minShards = gid, len(group2Shards[gid])
		}
	}
	return index
}

// groups 深拷贝
func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func group2Shards(config Config) map[int][]int {
	group2Shards := make(map[int][]int)
	for gid := range config.Groups {
		group2Shards[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		group2Shards[gid] = append(group2Shards[gid], shard)
	}
	return group2Shards
}
