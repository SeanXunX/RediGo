package kv

import (
	"sync"
	"time"
)

var KVStore sync.Map

type StoreValue struct {
	value     string
	px        int
	createdAt time.Time
}

func Set(key, value string) {
	v := StoreValue{
		value:     value,
		px:        -1,
		createdAt: time.Now(),
	}
	KVStore.Store(key, v)
}

func SetExpire(key, value string, t int) {
	v := StoreValue{
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
		v := v.(StoreValue)
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
