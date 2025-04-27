package kvraft

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if val, ok := memoryKV.KV[key]; ok {
		return val, Ok
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return Ok
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	if val, ok := memoryKV.KV[key]; ok {
		memoryKV.KV[key] = val + value
	} else {
		memoryKV.KV[key] = value
	}
	return Ok
}
