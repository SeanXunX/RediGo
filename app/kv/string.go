package kv

import "time"

type StringValue struct {
	value     string
	px        int
	createdAt time.Time
}

func (kv *KVStore) Set(key, value string) {
	v := StringValue{
		value:     value,
		px:        -1,
		createdAt: time.Now(),
	}
	kv.store(key, v, StringType)
}

func (kv *KVStore) SetExpire(key, value string, t int) {
	v := StringValue{
		value:     value,
		px:        t,
		createdAt: time.Now(),
	}
	kv.store(key, v, StringType)
}

func (kv *KVStore) Get(key string) (value any) {
	val, ok := kv.mp.Load(key)
	if !ok {
		return nil
	} else {
		v := val.(StoreValue).v.(StringValue)
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
