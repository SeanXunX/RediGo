package kv

import "sort"

type ZSetValue struct {
	memToScore map[string]float64
	scores     []float64
}

func NewEmptyZSetValue() ZSetValue {
	return ZSetValue{
		memToScore: make(map[string]float64),
		scores:     []float64{},
	}
}

func LowerBound(s []float64, tar float64) int {
	return sort.Search(len(s), func(i int) bool {
		return s[i] >= tar
	})
}

func (kv *KVStore) ZAdd(key string, member string, score float64) (isNew bool) {
	storeValAny, ok := kv.mp.Load(key)
	var newZSet ZSetValue
	if !ok {
		newZSet = NewEmptyZSetValue()
	} else {
		newZSet = storeValAny.(StoreValue).v.(ZSetValue)
	}

	// if member already exists, remove the original one
	if oldScore, ok := newZSet.memToScore[member]; ok {
		isNew = false
		delete(newZSet.memToScore, member)
		pos := LowerBound(newZSet.scores, oldScore)
		newZSet.scores = append(newZSet.scores[:pos], newZSet.scores[pos+1:]...)
	} else {
		isNew = true
	}

	// Insert member into the zset
	insertIdx := LowerBound(newZSet.scores, score)

	newZSet.scores = append(newZSet.scores, 0)
	copy(newZSet.scores[insertIdx+1:], newZSet.scores[insertIdx:])
	newZSet.scores[insertIdx] = score

	newZSet.memToScore[member] = score

	kv.store(key, newZSet, ZSetType)

	return
}
