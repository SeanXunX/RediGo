package kv

import (
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
	return len(value)
}
