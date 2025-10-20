package kv

import (
	"slices"
	"sync"
	"time"
)

var KVStore sync.Map

type SetValue struct {
	value     string
	px        int
	createdAt time.Time
}

type ListValue []string

func Set(key, value string) {
	v := SetValue{
		value:     value,
		px:        -1,
		createdAt: time.Now(),
	}
	KVStore.Store(key, v)
}

func SetExpire(key, value string, t int) {
	v := SetValue{
		value:     value,
		px:        t,
		createdAt: time.Now()}
	KVStore.Store(key, v)
}

func Get(key string) (value any) {
	v, ok := KVStore.Load(key)
	if !ok {
		return nil
	} else {
		v := v.(SetValue)
		if v.px == -1 {
			return v.value
		} else {
			duration := time.Duration(v.px) * time.Millisecond
			exTime := v.createdAt.Add(duration)
			if time.Now().Before(exTime) {
				return v.value
			} else {
				return nil
			}
		}
	}
}

func RPush(key string, value []string) int {
	oldTarList, ok := KVStore.Load(key)
	var newTarList ListValue
	if !ok {
		newTarList = ListValue{}
	} else {
		newTarList = oldTarList.(ListValue)
	}
	newTarList = append(newTarList, value...)
	KVStore.Store(key, newTarList)
	return len(newTarList)
}

func LPush(key string, value []string) int {
	oldTarList, ok := KVStore.Load(key)
	var newTarList ListValue
	if !ok {
		newTarList = ListValue{}
	} else {
		newTarList = oldTarList.(ListValue)
	}
	slices.Reverse(value)
	newTarList = append(value, newTarList...)
	KVStore.Store(key, newTarList)
	return len(newTarList)
}

func validateRange(start, stop, length int) (int, int) {
	if start < 0 {
		start = length + start
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = length + stop
	}
	if stop >= length {
		stop = length - 1
	}
	return start, stop
}

func LRange(key string, start, stop int) ListValue {
	res := ListValue{}
	tarListAny, ok := KVStore.Load(key)
	if !ok {
		return res
	} else {
		tarList := tarListAny.(ListValue)
		length := len(tarList)
		start, stop = validateRange(start, start, length)
		if start >= length || start > stop || stop < 0 {
			return res
		}
		res = tarListAny.(ListValue)[start : stop+1]
	}
	return res
}

func LLen(key string) int {
	tarListAny, ok := KVStore.Load(key)
	if !ok {
		return 0
	} else {
		tarList := tarListAny.(ListValue)
		return len(tarList)
	}
}

func LPop(key string) any {
	tarListAny, ok := KVStore.Load(key)
	if !ok {
		return nil
	}
	tarList := tarListAny.(ListValue)
	length := len(tarList)
	if length == 0 {
		return nil
	}
	res := tarList[0]
	tarList = tarList[1:]
	KVStore.Store(key, tarList)
	return res
}

func LPopN(key string, num int) []string {
	tarListAny, ok := KVStore.Load(key)
	if !ok {
		return nil
	}
	tarList := tarListAny.(ListValue)
	length := len(tarList)
	if length == 0 {
		return nil
	}
	if num > length {
		num = length
	}
	res := tarList[:num]
	tarList = tarList[num:]
	KVStore.Store(key, tarList)
	return res
}
