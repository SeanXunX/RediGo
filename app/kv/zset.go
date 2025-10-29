package kv

import (
	"sort"
)

type ZSetElem struct {
	member string
	score  float64
}

type ZSetValue struct {
	memToScore map[string]float64
	scores     []ZSetElem
}

func NewEmptyZSetValue() ZSetValue {
	return ZSetValue{
		memToScore: make(map[string]float64),
		scores:     []ZSetElem{},
	}
}

func LowerBound(s []ZSetElem, tar ZSetElem) int {
	return sort.Search(len(s), func(i int) bool {
		if s[i].score == tar.score {
			return s[i].member >= tar.member
		}
		return s[i].score > tar.score
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
		pos := LowerBound(newZSet.scores, ZSetElem{member, oldScore})
		newZSet.scores = append(newZSet.scores[:pos], newZSet.scores[pos+1:]...)
	} else {
		isNew = true
	}

	// Insert member into the zset
	newElem := ZSetElem{member, score}
	insertIdx := LowerBound(newZSet.scores, newElem)

	newZSet.scores = append(newZSet.scores, ZSetElem{})
	copy(newZSet.scores[insertIdx+1:], newZSet.scores[insertIdx:])
	newZSet.scores[insertIdx] = newElem

	newZSet.memToScore[member] = score

	kv.store(key, newZSet, ZSetType)

	return
}

// If the member, or the sorted set does not exist, return nil.
// Otherwise, return the rank.
func (kv *KVStore) ZRank(key string, member string) any {
	storeValAny, ok := kv.mp.Load(key)
	var zSet ZSetValue
	if !ok {
		return nil
	} else {
		zSet = storeValAny.(StoreValue).v.(ZSetValue)
	}

	var score float64
	if s, ok := zSet.memToScore[member]; !ok {
		return nil
	} else {
		score = s
	}

	pos := LowerBound(zSet.scores, ZSetElem{member, score})

	return pos
}

func (kv *KVStore) ZRange(key string, start, end int) (res []string) {
	storeValAny, ok := kv.mp.Load(key)
	var zSet ZSetValue
	if !ok {
		return
	} else {
		zSet = storeValAny.(StoreValue).v.(ZSetValue)
	}

	ss := zSet.scores
	length := len(ss)

	if start < 0 {
		start = length + start
	}
	start = max(0, start)

	if end < 0 {
		end = length + end
	}
	end = max(0, end)

	if start >= len(ss) || start > end {
		return
	}
	end = min(end, len(ss)-1)

	res = make([]string, end-start+1)
	for i := start; i <= end; i++ {
		res[i-start] = ss[i].member
	}
	return
}

func (kv *KVStore) ZCard(key string) int {
	storeValAny, ok := kv.mp.Load(key)
	var zSet ZSetValue
	if !ok {
		return 0
	} else {
		zSet = storeValAny.(StoreValue).v.(ZSetValue)
	}
	return len(zSet.scores)
}

// Get member's score. If existing, return float64. Else Return nil.
func (kv *KVStore) ZScore(key string, member string) any {
	storeValAny, ok := kv.mp.Load(key)
	var zSet ZSetValue
	if !ok {
		return nil
	} else {
		zSet = storeValAny.(StoreValue).v.(ZSetValue)
	}
	if score, ok := zSet.memToScore[member]; !ok {
		return nil
	} else {
		return score
	}
}
