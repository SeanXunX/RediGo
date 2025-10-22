package kv

import (
	"fmt"
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

func parseIDString(str string, lastID any) (StreamID, error) {
	var ms, seq int64
	var err error

	if str == "*" {
		// "*"
	} else {
		parts := strings.Split(str, "-")
		if len(parts) != 2 {
			return StreamID{}, fmt.Errorf("Invalid stream ID: %s", str)
		}
		ms, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return StreamID{}, fmt.Errorf("Invalid millisecond part: %s", parts[0])
		}

		if parts[1] == "*" {
			// "<ms>-*>"
			if lastID == nil {
				if ms == 0 {
					seq = 1
				} else {
					seq = 0
				}
			} else {
				seq = lastID.(StreamID).Seq + 1
			}
		} else {
			// "<ms>-<seq>"
			seq, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return StreamID{}, fmt.Errorf("Invalid sequence part: %s", parts[1])
			}
		}
	}
	return StreamID{Ms: ms, Seq: seq}, nil
}

func less(prev, cur StreamID) bool {
	if prev.Ms == cur.Ms {
		return prev.Seq < cur.Seq
	}
	return prev.Ms < cur.Ms
}

func (kv *KVStore) XAdd(key string, idStr string, data map[string]string) (res string, t ValueType) {

	var id StreamID
	tarStreamAny, ok := kv.mp.Load(key)
	var tarStream StreamValue
	if !ok {
		// Not existed. Create a new stream.
		tarStream = StreamValue{}
		id, _ = parseIDString(idStr, nil)
	} else {
		tarStream = tarStreamAny.(StoreValue).v.(StreamValue)
		id, _ = parseIDString(idStr, tarStream.lastID)
		lastID := tarStream.lastID
		if !less(lastID, id) {
			return "The ID specified in XADD is equal or smaller than the target stream top item", ErrorType
		}
	}

	if id.Ms == 0 && id.Seq == 0 {
		return "The ID specified in XADD must be greater than 0-0", ErrorType
	}

	tarStream.entries = append(tarStream.entries, StreamEntry{ID: id, Data: data})
	tarStream.lastID = id
	kv.store(key, tarStream, StreamType)
	resIdStr := fmt.Sprintf("%d-%d", id.Ms, id.Seq)
	return resIdStr, StringType
}
