package kv

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
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
	lastID  StreamID // The last stream id of the entry.
	entries []StreamEntry
}

func less(id1, id2 StreamID) bool {
	if id1.Ms == id2.Ms {
		return id1.Seq < id2.Seq
	}
	return id1.Ms < id2.Ms
}

func equal(id1, id2 StreamID) bool {
	return id1.Ms == id2.Ms && id1.Seq == id2.Seq
}

func parseIDString(str string, lastID any) (StreamID, error) {
	var ms, seq int64
	var err error

	if str == "*" {
		// "*"
		ms = time.Now().UnixMilli()
		if lastID != nil && lastID.(StreamID).Ms == ms {
			seq = lastID.(StreamID).Seq + 1
		} else {
			seq = 0
		}

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
				lastStremID := lastID.(StreamID)
				if lastStremID.Ms == ms {
					seq = lastStremID.Seq + 1
				} else {
					seq = 0
				}
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
	}

	if id.Ms == 0 && id.Seq == 0 {
		return "The ID specified in XADD must be greater than 0-0", ErrorType
	}

	if len(tarStream.entries) > 0 {
		lastID := tarStream.lastID
		if !less(lastID, id) {
			return "The ID specified in XADD is equal or smaller than the target stream top item", ErrorType
		}
	}

	tarStream.entries = append(tarStream.entries, StreamEntry{ID: id, Data: data})
	tarStream.lastID = id
	kv.store(key, tarStream, StreamType)

	kv.fanOutCond.Broadcast()
	// log.Println("[debug] broadcasted")

	resIdStr := fmt.Sprintf("%d-%d", id.Ms, id.Seq)
	return resIdStr, StringType
}

// Find the last stream id with ms.
func findLastStreamID(entries []StreamEntry, ms int64) StreamID {
	nextID := StreamID{
		Ms:  ms + 1,
		Seq: 0,
	}
	l, r := 0, len(entries)-1
	for l <= r {
		mid := (r-l)/2 + l
		if !less(nextID, entries[mid].ID) {
			r = mid - 1
		} else {
			l = mid + 1
		}
	}
	return entries[r].ID
}

// For the start ID, the sequence number defaults to 0.
// For the end ID, the sequence number defaults to the maximum sequence number.
func (kv *KVStore) parseRangeID(entries []StreamEntry, idStr string, isStart bool) StreamID {
	if idStr == "-" {
		return entries[0].ID
	}
	if idStr == "+" {
		return entries[len(entries)-1].ID
	}
	parts := strings.Split(idStr, "-")
	ms, _ := strconv.ParseInt(parts[0], 10, 64)
	var seq int64
	if len(parts) == 2 {
		// "<ms>-<seq>"
		seq, _ = strconv.ParseInt(parts[1], 10, 64)
	} else {
		// "<ms>"
		if isStart {
			seq = 0
		} else {
			seq = findLastStreamID(entries, ms).Seq
		}
	}
	return StreamID{Ms: ms, Seq: seq}
}

func (kv *KVStore) getEntries(key string) ([]StreamEntry, bool) {
	tarStreamAny, ok := kv.mp.Load(key)
	if !ok {
		return []StreamEntry{}, ok
	}
	return tarStreamAny.(StoreValue).v.(StreamValue).entries, ok
}

// Retrieves a range of entries from a stream. The range is inclusive.
func (kv *KVStore) XRange(key, id1, id2 string) []StreamEntry {
	entries, ok := kv.getEntries(key)
	if !ok {
		log.Printf("[error]: key (%s) does not exist", key)
		return []StreamEntry{}
	}

	start, end := kv.parseRangeID(entries, id1, true), kv.parseRangeID(entries, id2, false)

	startIdx := sort.Search(len(entries), func(i int) bool {
		res := !less(entries[i].ID, start)
		return res
	})

	endIdx := sort.Search(len(entries), func(i int) bool {
		return !less(entries[i].ID, end) && !equal(end, entries[i].ID)
	})

	return entries[startIdx:endIdx]
}

// XRead reads data from one or multiple streams.
func (kv *KVStore) XRead(
	keys []string,
	ids []string,
	cnt int,
	isBlock bool,
	timeout time.Duration,
) [][]StreamEntry {

	n := len(keys)
	res := make([][]StreamEntry, n)

	for {
		gottenRes := false
		for i := range n {
			entries, ok := kv.getEntries(keys[i])
			if !ok {
				log.Printf("[warning]: key (%s) does not exist", keys[i])
				continue
			}

			start := kv.parseRangeID(entries, ids[i], true)

			startIdx := sort.Search(len(entries), func(i int) bool {
				res := !less(entries[i].ID, start) && !equal(start, entries[i].ID)
				return res
			})

			if startIdx != len(entries) {
				gottenRes = true
			}

			endIdx := len(entries)

			if cnt > 0 {
				endIdx = startIdx + cnt
			}
			res[i] = entries[startIdx:endIdx]
		}

		if gottenRes || !isBlock {
			return res
		}

		// block
		tCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		waitCh := make(chan struct{})
		// log.Println("[debug] Before go")
		go func() {
			kv.Lock()
			kv.fanOutCond.Wait()
			waitCh <- struct{}{}
			// log.Println("[debug] go func waked up")
			kv.Unlock()
		}()
		// log.Println("[debug] Before select")
		select {
		case <-tCtx.Done():
			return nil
		case <-waitCh:
			// log.Println("[debug] waitCh wakes up")
		}
	}
}
