package handler

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/kv"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type ConnHandler struct {
	conn    net.Conn
	in      chan CMD
	kvStore *kv.KVStore

	inTransaction bool
	commandQueue  []CMD

	serverInfo map[string]string
}

type CMD struct {
	Command string
	Args    []string
}

func NewConnHandler(conn net.Conn, store *kv.KVStore, serverInfo map[string]string) *ConnHandler {
	return &ConnHandler{
		conn:          conn,
		in:            make(chan CMD),
		kvStore:       store,
		inTransaction: false,
		commandQueue:  []CMD{},
		serverInfo:    serverInfo,
	}
}

func (h *ConnHandler) close() {
	h.conn.Close()
}

func (h *ConnHandler) Handle() {
	defer h.close()

	go h.readCMD()

	for cmd := range h.in {
		res := h.run(cmd)
		h.conn.Write(res)
	}
}

func (h *ConnHandler) run(cmd CMD) []byte {

	if h.inTransaction && !resp.CmpStrNoCase(cmd.Command, "EXEC") && !resp.CmpStrNoCase(cmd.Command, "DISCARD") {
		h.commandQueue = append(h.commandQueue, cmd)
		return resp.EncodeSimpleString("QUEUED")
	}

	switch strings.ToUpper(cmd.Command) {
	case "COMMAND":
		return []byte("*0\r\n")
	case "PING":
		return []byte("+PONG\r\n")
	case "ECHO":
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.Args[0]), cmd.Args[0]))
	case "SET":
		key, value := cmd.Args[0], cmd.Args[1]
		if len(cmd.Args) == 2 {
			h.kvStore.Set(key, value)
		} else if len(cmd.Args) == 4 {
			t, _ := strconv.Atoi(cmd.Args[3])
			if strings.ToUpper(cmd.Args[2]) == "EX" {
				t *= 1000
			}
			h.kvStore.SetExpire(key, value, t)
		}
		return []byte("+OK\r\n")
	case "GET":
		key := cmd.Args[0]
		val := h.kvStore.Get(key)
		if val != nil {
			return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val.(string)), val.(string)))
		} else {
			return []byte("$-1\r\n")
		}
	case "RPUSH":
		key := cmd.Args[0]
		value := cmd.Args[1:]
		length := h.kvStore.RPush(key, value)
		return resp.EncodeInt(length)
	case "LPUSH":
		key := cmd.Args[0]
		value := cmd.Args[1:]
		length := h.kvStore.LPush(key, value)
		return resp.EncodeInt(length)
	case "LRANGE":
		key := cmd.Args[0]
		start, _ := strconv.Atoi(cmd.Args[1])
		stop, _ := strconv.Atoi(cmd.Args[2])
		l := h.kvStore.LRange(key, start, stop)
		return resp.EncodeArray(l)
	case "LLEN":
		key := cmd.Args[0]
		length := h.kvStore.LLen(key)
		return resp.EncodeInt(length)
	case "LPOP":
		return h.handleLPOP(cmd)
	case "BLPOP":
		return h.handleBLPOP(cmd)
	case "TYPE":
		return h.handleType(cmd)
	case "XADD":
		return h.handleXADD(cmd)
	case "XRANGE":
		return h.handleXRANGE(cmd)
	case "XREAD":
		return h.handleXREAD(cmd)
	case "INCR":
		return h.handleINCR(cmd)
	case "MULTI":
		return h.handleMULTI()
	case "EXEC":
		return h.handleEXEC()
	case "DISCARD":
		return h.handleDISCARD()
	case "INFO":
		return h.handleINFO(cmd)
	default:
		return []byte{}
	}
}

func (h *ConnHandler) readCMD() {
	reader := bufio.NewReader(h.conn)
	for {
		parts, err := resp.DecodeArray(reader)
		if err == io.EOF {
			log.Println("Received EOF")
			return
		} else if err != nil {
			log.Println(err.Error())
			continue
		}
		cmd := CMD{}
		if len(parts) > 0 {
			cmd.Command = parts[0]
			if len(parts) > 1 {
				cmd.Args = parts[1:]
			}
			h.in <- cmd
		}
	}
}

func (h *ConnHandler) handleLPOP(cmd CMD) []byte {
	key := cmd.Args[0]
	var elem any

	switch len(cmd.Args) {
	case 1:
		elem = h.kvStore.LPop(key)
	case 2:
		num, _ := strconv.Atoi(cmd.Args[1])
		elem = h.kvStore.LPopN(key, num)
	}

	if elem == nil {
		return resp.EncodeNullBulkString()
	}

	switch v := elem.(type) {
	case string:
		return resp.EncodeBulkString(v)
	case []string:
		return resp.EncodeArray(v)
	}
	return []byte{}
}

func (h *ConnHandler) handleBLPOP(cmd CMD) []byte {
	key := cmd.Args[0]
	seconds, ok := strconv.ParseFloat(cmd.Args[1], 64)
	if ok != nil {
		log.Printf("Unexpected string for float64 waiting time: %s\n", cmd.Args[1])
	}
	timeout := time.Duration(seconds * float64(time.Second))

	elem := h.kvStore.BLPop(key, timeout)
	if elem == nil {
		return resp.EncodeNullArray()
	}
	res := []string{key, elem.(string)}
	return resp.EncodeArray(res)
}

func (h *ConnHandler) handleType(cmd CMD) []byte {
	key := cmd.Args[0]
	t := h.kvStore.Type(key)
	return resp.EncodeSimpleString(t)
}

func (h *ConnHandler) handleXADD(cmd CMD) []byte {
	key := cmd.Args[0]
	id := cmd.Args[1]
	data := map[string]string{}

	for i := 2; i < len(cmd.Args); i += 2 {
		k, v := cmd.Args[i], cmd.Args[i+1]
		data[k] = v
	}
	res, t := h.kvStore.XAdd(key, id, data)
	if t == kv.ErrorType {
		return resp.EncodeSimpleError(res)
	} else if t == kv.StringType {
		return resp.EncodeBulkString(res)
	} else {
		return []byte{}
	}
}

func (h *ConnHandler) handleXRANGE(cmd CMD) []byte {
	key := cmd.Args[0]
	id1, id2 := cmd.Args[1], cmd.Args[2]

	resEntries := h.kvStore.XRange(key, id1, id2)
	return resp.EncodeStreamEntries(resEntries)
}

func (h *ConnHandler) handleXREAD(cmd CMD) []byte {
	count := -1
	if resp.CmpStrNoCase(cmd.Args[0], "COUNT") {
		count, _ = strconv.Atoi(cmd.Args[1])
	}

	isBlock := false
	var timeout time.Duration

	if resp.CmpStrNoCase(cmd.Args[0], "BLOCK") {
		isBlock = true
		ms, _ := strconv.Atoi(cmd.Args[1])
		timeout = time.Duration(ms) * time.Millisecond
	}
	if resp.CmpStrNoCase(cmd.Args[2], "BLOCK") {
		isBlock = true
		ms, _ := strconv.Atoi(cmd.Args[3])
		timeout = time.Duration(ms) * time.Millisecond
	}

	findSTREAMS := func() int {
		for i, s := range cmd.Args {
			if resp.CmpStrNoCase(s, "STREAMS") {
				return i
			}
		}
		return -1
	}
	if baseIdx := findSTREAMS(); baseIdx != -1 {
		num := (len(cmd.Args) - 1 - baseIdx) / 2
		keys := make([]string, num)
		ids := make([]string, num)
		for i := range num {
			keys[i] = cmd.Args[baseIdx+i+1]
			ids[i] = cmd.Args[baseIdx+num+i+1]
		}
		resEntries := h.kvStore.XRead(keys, ids, count, isBlock, timeout)
		if resEntries == nil {
			return resp.EncodeNullArray()
		}
		return resp.EncodeStreamEntriesWithKeys(keys, resEntries)
	}
	return []byte{}
}

func (h *ConnHandler) handleINCR(cmd CMD) []byte {
	key := cmd.Args[0]
	res, t := h.kvStore.Incr(key)

	if t == kv.ErrorType {
		return resp.EncodeSimpleError(res.(string))
	}

	return resp.EncodeInt64(res.(int64))
}

func (h *ConnHandler) handleMULTI() []byte {
	h.inTransaction = true
	return []byte("+OK\r\n")
}

func (h *ConnHandler) handleEXEC() []byte {
	if !h.inTransaction {
		return resp.EncodeSimpleError("EXEC without MULTI")

	}
	defer func() {
		h.inTransaction = false
		h.commandQueue = h.commandQueue[len(h.commandQueue):]
	}()
	if len(h.commandQueue) == 0 {
		return resp.EncodeEmptyArray()
	}

	h.inTransaction = false

	res := []byte{}
	res = fmt.Appendf(res, "*%d\r\n", len(h.commandQueue))
	for _, cmd := range h.commandQueue {
		res = append(res, h.run(cmd)...)
	}
	return res
}

func (h *ConnHandler) handleDISCARD() []byte {
	if !h.inTransaction {
		return resp.EncodeSimpleError("DISCARD without MULTI")
	}
	h.inTransaction = false
	h.commandQueue = h.commandQueue[len(h.commandQueue):]
	return []byte("+OK\r\n")
}

func (h *ConnHandler) handleINFO(cmd CMD) []byte {
	res := []byte{}
	if len(cmd.Args) > 0 {
		switch strings.ToLower(cmd.Args[0]) {
		case "replication":
			infoStr := fmt.Sprintf(`# Replication
role:%s
master_replid:%s
master_repl_offset:%s
`,
				h.serverInfo["role"],
				h.serverInfo["master_replid"],
				h.serverInfo["master_repl_offset"],
			)

			res = resp.EncodeBulkString(infoStr)
		}
	}
	return res
}
