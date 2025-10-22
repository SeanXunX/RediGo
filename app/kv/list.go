package kv

import (
	"context"
	"fmt"
	"slices"
	"time"
)

type ListValue []string

func (kv *KVStore) wake(key string) {
	kv.Lock()
	defer kv.Unlock()
	wQ, ok := kv.watingQueue[key]
	if !ok || len(wQ) == 0 {
		return
	}
	ch := wQ[0]
	kv.watingQueue[key] = wQ[1:]
	ch <- struct{}{}
}

func (kv *KVStore) RPush(key string, value []string) int {
	oldTarList, ok := kv.mp.Load(key)
	var newTarList ListValue
	if !ok {
		newTarList = ListValue{}
	} else {
		newTarList = oldTarList.(StoreValue).v.(ListValue)
	}
	newTarList = append(newTarList, value...)
	kv.store(key, newTarList, ListType)
	kv.wake(key)
	return len(newTarList)
}

func (kv *KVStore) LPush(key string, value []string) int {
	oldTarList, ok := kv.mp.Load(key)
	var newTarList ListValue
	if !ok {
		newTarList = ListValue{}
	} else {
		newTarList = oldTarList.(StoreValue).v.(ListValue)
	}
	slices.Reverse(value)
	newTarList = append(value, newTarList...)
	kv.store(key, newTarList, ListType)
	kv.wake(key)
	return len(newTarList)
}

func (kv *KVStore) validateRange(start, stop, length int) (int, int) {
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

func (kv *KVStore) LRange(key string, start, stop int) ListValue {
	res := ListValue{}
	tarListAny, ok := kv.mp.Load(key)
	if !ok {
		return res
	} else {
		tarList := tarListAny.(StoreValue).v.(ListValue)
		length := len(tarList)
		start, stop = kv.validateRange(start, stop, length)
		if start >= length || start > stop || stop < 0 {
			return res
		}
		res = tarList[start : stop+1]
	}
	return res
}

func (kv *KVStore) LLen(key string) int {
	tarListAny, ok := kv.mp.Load(key)
	if !ok {
		return 0
	} else {
		tarList := tarListAny.(StoreValue).v.(ListValue)
		return len(tarList)
	}
}

func (kv *KVStore) LPop(key string) any {
	tarListAny, ok := kv.mp.Load(key)
	if !ok {
		return nil
	}
	tarList := tarListAny.(StoreValue).v.(ListValue)
	length := len(tarList)
	if length == 0 {
		return nil
	}
	res := tarList[0]
	tarList = tarList[1:]
	kv.store(key, tarList, ListType)
	return res
}

func (kv *KVStore) LPopN(key string, num int) []string {
	tarListAny, ok := kv.mp.Load(key)
	if !ok {
		return nil
	}
	tarList := tarListAny.(StoreValue).v.(ListValue)
	length := len(tarList)
	if length == 0 {
		return nil
	}
	if num > length {
		num = length
	}
	res := tarList[:num]
	tarList = tarList[num:]
	kv.store(key, tarList, ListType)
	return res
}

func (kv *KVStore) BLPop(key string, timeout time.Duration) any {
	if tarListAny, ok := kv.mp.Load(key); ok {
		tarList := tarListAny.(StoreValue).v.(ListValue)
		length := len(tarList)
		if length > 0 {
			res := tarList[0]
			tarList = tarList[1:]
			kv.store(key, tarList, ListType)
			return res
		}
	}

	kv.Lock()
	if _, ok := kv.watingQueue[key]; !ok {
		wQ := []chan struct{}{}
		kv.watingQueue[key] = wQ
	}

	ch := make(chan struct{})
	kv.watingQueue[key] = append(kv.watingQueue[key], ch)
	kv.Unlock()

	var (
		tCtx   context.Context
		cancel context.CancelFunc
	)
	if timeout == 0 {
		tCtx = context.Background()
	} else {
		tCtx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	}

	select {
	case <-tCtx.Done():
		return nil
	case <-ch:
	}

	tarListAny, ok := kv.mp.Load(key)
	if !ok {
		panic(fmt.Sprintf("Unexpected empty tarList for key: %s", key))
	}
	tarList := tarListAny.(StoreValue).v.(ListValue)
	length := len(tarList)
	if length == 0 {
		panic(fmt.Sprintf("Unexpected empty tarList for key: %s", key))
	}
	res := tarList[0]
	tarList = tarList[1:]
	kv.store(key, tarList, ListType)
	return res
}
