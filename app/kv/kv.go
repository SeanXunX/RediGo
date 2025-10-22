package kv

import (
	"sync"
)

type KVStore struct {
	sync.Mutex
	mp          sync.Map
	watingQueue map[string][]chan struct{}
}

type ValueType int

const (
	StringType ValueType = iota
	ListType
	SetType
	ZSetType
	HashType
	StreamType
	VectorsetType
)

type StoreValue struct {
	t ValueType // type
	v any       // val
}

func NewKVStore() *KVStore {
	return &KVStore{
		mp:          sync.Map{},
		watingQueue: make(map[string][]chan struct{}),
	}
}

func (kv *KVStore) store(key string, val any, t ValueType) {
	storeV := StoreValue{
		t: t,
		v: val,
	}
	kv.mp.Store(key, storeV)
}

func (kv *KVStore) Type(key string) string {
	val, ok := kv.mp.Load(key)
	if !ok {
		return "none"
	}
	switch val.(StoreValue).t {
	case StringType:
		return "string"
	case ListType:
		return "list"
	case SetType:
		return "set"
	case ZSetType:
		return "zset"
	case HashType:
		return "hash"
	case StreamType:
		return "stream"
	case VectorsetType:
		return "vectorset"
	default:
		return "none"
	}
}
