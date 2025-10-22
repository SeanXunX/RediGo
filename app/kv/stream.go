package kv

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

type StreamID struct {
	Ms  int64
	Seq int64
}

type StreamEntry struct {
	ID   StreamID
	Data map[string]string
}

type StreamValue struct {
	lastID  StreamID
	entries []StreamEntry
}

func parseIDString(str string) (StreamID, error) {
	if str == "*" {
		return StreamID{}, nil
	} else {
		parts := strings.Split(str, "-")
		if len(parts) != 2 {
			return StreamID{}, fmt.Errorf("Invalid stream ID: %s", str)
		}
		ms, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return StreamID{}, fmt.Errorf("Invalid millisecond part: %s", parts[0])
		}
		if parts[1] == "*" {
			return StreamID{}, nil
		} else {
			seq, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return StreamID{}, fmt.Errorf("Invalid sequence part: %s", parts[1])
			}
			return StreamID{Ms: ms, Seq: seq}, nil
		}
	}
}

func less(prev, cur StreamID) bool {
	if prev.Ms == cur.Ms {
		return prev.Seq < cur.Seq
	}
	return prev.Ms < cur.Ms
}

func (kv *KVStore) XAdd(key string, idStr string, data map[string]string) (res string, t ValueType) {
	id, err := parseIDString(idStr)
	if err != nil {
		log.Println(err.Error())
	}

	tarStreamAny, ok := kv.mp.Load(key)
	var tarStream StreamValue
	if !ok {
		// Not existed. Create a new stream.
		tarStream = StreamValue{}
	} else {
		tarStream = tarStreamAny.(StoreValue).v.(StreamValue)
		fmt.Println(tarStream.lastID, id)
		if !less(tarStream.lastID, id) {
			return "The ID specified in XADD is equal or smaller than the target stream top item", ErrorType
		}
	}
	tarStream.entries = append(tarStream.entries, StreamEntry{ID: id, Data: data})
	tarStream.lastID = id
	kv.store(key, tarStream, StreamType)
	return idStr, StringType
}
