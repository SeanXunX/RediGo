package kv

import (
	"sync"
)

type KVStore struct {
	sync.Mutex
	mp          sync.Map
	watingQueue map[string][]chan struct{} // exclusive chan for each blpop client
	fanOutCond  *sync.Cond                 // fan-out sync for all xread blocked at same stream
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
	ErrorType
)

type StoreValue struct {
	t ValueType // type
	v any       // val
}

func NewStoreValue(t ValueType, v any) StoreValue {
	return StoreValue{t, v}
}

func NewKVStore() *KVStore {
	kv := &KVStore{
		mp:          sync.Map{},
		watingQueue: make(map[string][]chan struct{}),
	}
	kv.fanOutCond = sync.NewCond(&kv.Mutex)
	return kv
}

func (kv *KVStore) store(key string, val any, t ValueType) {
	storeV := StoreValue{
		t: t,
		v: val,
	}
	kv.mp.Store(key, storeV)
}

func (kv *KVStore) Store(key string, sVal StoreValue) {
	kv.mp.Store(key, sVal)
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

func (kv *KVStore) Keys(query string) []string {
	keys := []string{}
	kv.mp.Range(func(k, v any) bool {
		keys = append(keys, k.(string))
		return true
	})
	return keys
}
