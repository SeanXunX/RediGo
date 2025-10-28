package server

import (
	"bufio"
	"encoding/base64"
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
	conn net.Conn
	in   chan CMD

	inTransaction bool
	commandQueue  []CMD

	s *Server
}

type CMD struct {
	Command   string
	Args      []string
	RespBytes int
}

var writeCommands = map[string]bool{
	"SET":      true,
	"DEL":      true,
	"INCR":     true,
	"DECR":     true,
	"RPUSH":    true,
	"LPUSH":    true,
	"LPOP":     true,
	"RPOP":     true,
	"XADD":     true,
	"HMSET":    true,
	"HSET":     true,
	"HDEL":     true,
	"EXPIRE":   true,
	"PEXPIRE":  true,
	"FLUSHDB":  true,
	"FLUSHALL": true,
}

var subModeCommands = map[string]bool{
	"SUBSCRIBE":    true,
	"UNSUBSCRIBE":  true,
	"PSUBSCRIBE":   true,
	"PUNSUBSCRIBE": true,
	"PING":         true,
	"QUIT":         true,
}

func NewConnHandler(conn net.Conn, s *Server) *ConnHandler {
	return &ConnHandler{
		conn:          conn,
		in:            make(chan CMD),
		inTransaction: false,
		commandQueue:  []CMD{},
		s:             s,
	}
}

func (h *ConnHandler) close() {
	h.conn.Close()
}

// Slave should return its reponse when receiving "REPLCONF GETACK" from master
func isReplGetAck(cmd CMD) bool {
	return strings.EqualFold(cmd.Command, "REPLCONF") && strings.EqualFold(cmd.Args[0], "GETACK")
}

func (h *ConnHandler) Handle(isSlave bool) {
	defer h.close()

	// Read commands from clients. Read from `h.conn`
	go h.readCMD()

	for cmd := range h.in {
		// master propagate write commands to its slavers
		h.propagateCMD(cmd)

		// execute cmd
		res := h.run(cmd)

		// Master or specific commands should write back
		if !isSlave || isReplGetAck(cmd) {
			h.conn.Write(res)
		}

		// increment slave received bytes
		h.s.SlaveReplOffset += cmd.RespBytes
	}
}

func (h *ConnHandler) isInSubMode() bool {
	psMan := h.s.PubSub

	psMan.mu.RLock()
	_, ok := psMan.subscribers[h.conn]
	psMan.mu.RUnlock()

	if !ok {
		return false
	}

	psMan.mu.RLock()
	sub := psMan.subscribers[h.conn]
	psMan.mu.RUnlock()

	sub.mu.RLock()
	cnt := len(sub.Channels)
	sub.mu.RUnlock()

	if cnt > 0 {
		return true
	}

	return false
}

func isSubModCommand(cmd CMD) bool {
	return subModeCommands[strings.ToUpper(cmd.Command)]
}

func (h *ConnHandler) propagateCMD(cmd CMD) {
	if !writeCommands[strings.ToUpper(cmd.Command)] {
		return
	}
	h.s.SlaveMu.RLock()
	for _, slave := range h.s.SlaveConns {
		strs := append([]string{cmd.Command}, cmd.Args...)
		slave.Write(resp.EncodeArray(strs))
	}
	h.s.SlaveMu.RUnlock()

	h.s.MasterOffsetMu.Lock()
	h.s.MasterReplOffset += cmd.RespBytes
	h.s.MasterOffsetMu.Unlock()
}

func (h *ConnHandler) readCMD() {
	reader := bufio.NewReader(h.conn)
	for {
		parts, n, err := resp.DecodeArray(reader)
		if err == io.EOF {
			log.Println("Received EOF")
			return
		} else if err != nil {
			log.Println(err.Error())
			continue
		}
		cmd := CMD{RespBytes: n}
		if len(parts) > 0 {
			cmd.Command = parts[0]
			if len(parts) > 1 {
				cmd.Args = parts[1:]
			}
			h.in <- cmd
		}
	}
}

func (h *ConnHandler) run(cmd CMD) []byte {
	isSubMode := h.isInSubMode()
	if isSubMode && !isSubModCommand(cmd) {
		return resp.EncodeSimpleError(
			fmt.Sprintf("Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", cmd.Command),
		)
	}

	if h.inTransaction && !resp.CmpStrNoCase(cmd.Command, "EXEC") && !resp.CmpStrNoCase(cmd.Command, "DISCARD") {
		h.commandQueue = append(h.commandQueue, cmd)
		return resp.EncodeSimpleString("QUEUED")
	}

	switch strings.ToUpper(cmd.Command) {
	case "COMMAND":
		return []byte("*0\r\n")
	case "PING":
		return h.handlePING(isSubMode)
	case "ECHO":
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.Args[0]), cmd.Args[0]))
	case "SET":
		key, value := cmd.Args[0], cmd.Args[1]
		if len(cmd.Args) == 2 {
			h.s.KVStore.Set(key, value)
		} else if len(cmd.Args) == 4 {
			t, _ := strconv.Atoi(cmd.Args[3])
			if strings.ToUpper(cmd.Args[2]) == "EX" {
				t *= 1000
			}
			h.s.KVStore.SetExpire(key, value, t)
		}
		return []byte("+OK\r\n")
	case "GET":
		key := cmd.Args[0]
		val := h.s.KVStore.Get(key)
		if val != nil {
			return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val.(string)), val.(string)))
		} else {
			return []byte("$-1\r\n")
		}
	case "RPUSH":
		key := cmd.Args[0]
		value := cmd.Args[1:]
		length := h.s.KVStore.RPush(key, value)
		return resp.EncodeInt(length)
	case "LPUSH":
		key := cmd.Args[0]
		value := cmd.Args[1:]
		length := h.s.KVStore.LPush(key, value)
		return resp.EncodeInt(length)
	case "LRANGE":
		key := cmd.Args[0]
		start, _ := strconv.Atoi(cmd.Args[1])
		stop, _ := strconv.Atoi(cmd.Args[2])
		l := h.s.KVStore.LRange(key, start, stop)
		return resp.EncodeArray(l)
	case "LLEN":
		key := cmd.Args[0]
		length := h.s.KVStore.LLen(key)
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
	case "REPLCONF":
		return h.handleREPLCONF(cmd)
	case "PSYNC":
		return h.handlePSYNC()
	case "WAIT":
		return h.handleWAIT(cmd)
	case "CONFIG":
		return h.handleCONFIG(cmd)
	case "KEYS":
		return h.handleKEYS(cmd)
	case "SUBSCRIBE":
		return h.handleSUBSCRIBE(cmd)
	case "PUBLISH":
		return h.handlePUBLISH(cmd)
	case "UNSUBSCRIBE":
		return h.handleUNSUBSCRIBE(cmd)
	case "ZADD":
		return h.handleZADD(cmd)
	case "ZRANK":
		return h.handleZRANK(cmd)
	case "ZRANGE":
		return h.handleZRANGE(cmd)
	case "ZCARD":
		return h.handleZCARD(cmd)
	default:
		return []byte{}
	}
}

func (h *ConnHandler) handlePING(isSubMod bool) []byte {
	if !isSubMod {
		return []byte("+PONG\r\n")
	} else {
		return resp.EncodeArray([]string{"pong", ""})
	}
}

func (h *ConnHandler) handleLPOP(cmd CMD) []byte {
	key := cmd.Args[0]
	var elem any

	switch len(cmd.Args) {
	case 1:
		elem = h.s.KVStore.LPop(key)
	case 2:
		num, _ := strconv.Atoi(cmd.Args[1])
		elem = h.s.KVStore.LPopN(key, num)
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

	elem := h.s.KVStore.BLPop(key, timeout)
	if elem == nil {
		return resp.EncodeNullArray()
	}
	res := []string{key, elem.(string)}
	return resp.EncodeArray(res)
}

func (h *ConnHandler) handleType(cmd CMD) []byte {
	key := cmd.Args[0]
	t := h.s.KVStore.Type(key)
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
	res, t := h.s.KVStore.XAdd(key, id, data)
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

	resEntries := h.s.KVStore.XRange(key, id1, id2)
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
		resEntries := h.s.KVStore.XRead(keys, ids, count, isBlock, timeout)
		if resEntries == nil {
			return resp.EncodeNullArray()
		}
		return resp.EncodeStreamEntriesWithKeys(keys, resEntries)
	}
	return []byte{}
}

func (h *ConnHandler) handleINCR(cmd CMD) []byte {
	key := cmd.Args[0]
	res, t := h.s.KVStore.Incr(key)

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
			h.s.MasterOffsetMu.RLock()
			infoStr := fmt.Sprintf(`# Replication
role:%s
master_replid:%s
master_repl_offset:%d
`,
				h.s.Role,
				h.s.MasterReplId,
				h.s.MasterReplOffset,
			)
			h.s.MasterOffsetMu.RUnlock()

			res = resp.EncodeBulkString(infoStr)
		}
	}
	return res
}

func (h *ConnHandler) handleREPLCONF(cmd CMD) []byte {
	// slave
	if strings.EqualFold(cmd.Args[0], "GETACK") && strings.EqualFold(cmd.Args[1], "*") {
		return resp.EncodeArray([]string{"REPLCONF", "ACK", fmt.Sprintf("%d", h.s.SlaveReplOffset)})
	}
	if strings.EqualFold(cmd.Args[0], "ACK") {

		offset, _ := strconv.Atoi(cmd.Args[1])

		h.s.MasterOffsetMu.RLock()
		defer h.s.MasterOffsetMu.RUnlock()
		if offset >= h.s.MasterReplOffset {

			h.s.ackMu.Lock()
			h.s.ackCnt++
			h.s.ackMu.Unlock()
		}
		return []byte{}
	}
	return []byte("+OK\r\n")
}

func (h *ConnHandler) handlePSYNC() []byte {
	res := []byte{}
	res = append(res, resp.EncodeSimpleString(fmt.Sprintf("FULLRESYNC %s 0", h.s.MasterReplId))...)
	base64Content := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
	rdbBytes, err := base64.StdEncoding.DecodeString(base64Content)
	if err != nil {
		log.Print(err.Error())
		return res
	}
	res = append(res, resp.EncodeRDBFile(rdbBytes)...)

	h.s.SlaveMu.Lock()
	h.s.SlaveConns = append(h.s.SlaveConns, h.conn)
	h.s.SlaveMu.Unlock()

	return res
}

func (h *ConnHandler) handleWAIT(cmd CMD) []byte {
	numReplcas, err := strconv.Atoi(cmd.Args[0])

	if numReplcas == 0 {
		return resp.EncodeInt(0)
	}

	if err != nil {
		log.Print(err.Error())
		return []byte{}
	}

	h.s.MasterOffsetMu.RLock()
	masOff := h.s.MasterReplOffset
	h.s.MasterOffsetMu.RUnlock()

	if masOff == 0 {
		h.s.SlaveMu.RLock()
		defer h.s.SlaveMu.RUnlock()
		return resp.EncodeInt(len(h.s.SlaveConns))
	}

	timeoutMs, err := strconv.Atoi(cmd.Args[1])
	timeout := time.Millisecond * time.Duration(timeoutMs)

	// Reset ack count
	h.s.ackMu.Lock()
	h.s.ackCnt = 0
	h.s.ackMu.Unlock()

	// Send getack to slaves
	getAckBytes := resp.EncodeArray([]string{"REPLCONF", "GETACK", "*"})
	defer func() {
		h.s.MasterOffsetMu.Lock()
		h.s.MasterReplOffset += len(getAckBytes)
		h.s.MasterOffsetMu.Unlock()
	}()

	h.s.SlaveMu.RLock()
	for _, slaveConn := range h.s.SlaveConns {
		slaveConn.Write(getAckBytes)
	}
	h.s.SlaveMu.RUnlock()

	// Time stopper
	timeoutCh := time.After(timeout)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCh:
			h.s.ackMu.RLock()
			defer h.s.ackMu.RUnlock()
			return resp.EncodeInt(h.s.ackCnt)
		case <-ticker.C:
			h.s.ackMu.RLock()
			curCnt := h.s.ackCnt
			h.s.ackMu.RUnlock()
			if curCnt >= numReplcas {
				return resp.EncodeInt(curCnt)
			}
		}
	}
}

func (h *ConnHandler) handleCONFIG(cmd CMD) []byte {
	fmt.Printf("[debug] received cmd = %+v", cmd)
	if cmd.Args[0] == "GET" {
		switch param := cmd.Args[1]; param {
		case "dir":
			return resp.EncodeArray([]string{"dir", h.s.Dir})
		case "dbfilename":
			return resp.EncodeArray([]string{"dbfilename", h.s.Dbfilename})
		}
	}
	return []byte{}
}

func (h *ConnHandler) handleKEYS(cmd CMD) []byte {
	query := cmd.Args[0]
	keys := h.s.KVStore.Keys(query)
	return resp.EncodeArray(keys)
}

func (h *ConnHandler) handleSUBSCRIBE(cmd CMD) []byte {
	chName := cmd.Args[0]

	psMan := h.s.PubSub

	psMan.mu.RLock()
	_, ok := psMan.subscribers[h.conn]
	psMan.mu.RUnlock()

	if !ok {
		psMan.mu.Lock()
		psMan.subscribers[h.conn] = NewSubscriber()
		psMan.mu.Unlock()
	}

	psMan.mu.Lock()
	if _, ok := psMan.channels[chName]; !ok {
		psMan.channels[chName] = make(map[net.Conn]*Subscriber)
	}
	psMan.channels[chName][h.conn] = psMan.subscribers[h.conn]
	psMan.mu.Unlock()

	psMan.mu.RLock()
	sub := psMan.subscribers[h.conn]
	psMan.mu.RUnlock()

	sub.mu.RLock()
	_, ok = sub.Channels[chName]
	sub.mu.RUnlock()

	if !ok {
		sub.mu.Lock()
		sub.Channels[chName] = true
		sub.mu.Unlock()
	}

	sub.mu.RLock()
	cnt := len(sub.Channels)
	sub.mu.RUnlock()

	res := fmt.Appendf([]byte{}, "*%d\r\n", 3)
	res = append(res, resp.EncodeBulkString("subscribe")...)
	res = append(res, resp.EncodeBulkString(chName)...)
	res = append(res, resp.EncodeInt(cnt)...)
	return res
}

func (h *ConnHandler) handlePUBLISH(cmd CMD) []byte {
	chName, msg := cmd.Args[0], cmd.Args[1]

	encodedMsg := resp.EncodeArray([]string{"message", chName, msg})

	// Get all the clients conn
	psMan := h.s.PubSub

	psMan.mu.RLock()
	cnt := len(psMan.channels[chName])
	for conn := range psMan.channels[chName] {
		conn.Write(encodedMsg)
	}
	psMan.mu.RUnlock()

	return resp.EncodeInt(cnt)
}

func (h *ConnHandler) handleUNSUBSCRIBE(cmd CMD) []byte {
	chName := cmd.Args[0]

	psMan := h.s.PubSub

	psMan.mu.Lock()
	delete(psMan.channels[chName], h.conn)
	psMan.mu.Unlock()

	psMan.mu.RLock()
	sub := psMan.subscribers[h.conn]
	psMan.mu.RUnlock()

	sub.mu.Lock()
	delete(sub.Channels, chName)
	sub.mu.Unlock()

	sub.mu.RLock()
	cnt := len(sub.Channels)
	sub.mu.RUnlock()

	res := fmt.Appendf([]byte{}, "*%d\r\n", 3)
	res = append(res, resp.EncodeBulkString("unsubscribe")...)
	res = append(res, resp.EncodeBulkString(chName)...)
	res = append(res, resp.EncodeInt(cnt)...)
	return res

}

func (h *ConnHandler) handleZADD(cmd CMD) []byte {
	key := cmd.Args[0]
	score, err := strconv.ParseFloat(cmd.Args[1], 64)
	if err != nil {
		log.Println(err.Error())
		return []byte{}
	}
	member := cmd.Args[2]
	isNew := h.s.KVStore.ZAdd(key, member, score)
	if isNew {
		return resp.EncodeInt(1)
	} else {
		return resp.EncodeInt(0)
	}
}

func (h *ConnHandler) handleZRANK(cmd CMD) []byte {
	key := cmd.Args[0]
	member := cmd.Args[1]

	rnk := h.s.KVStore.ZRank(key, member)
	if rnk == nil {
		return resp.EncodeNullBulkString()
	} else {
		return resp.EncodeInt(rnk.(int))
	}
}

func (h *ConnHandler) handleZRANGE(cmd CMD) []byte {
	key := cmd.Args[0]

	start, err := strconv.Atoi(cmd.Args[1])
	if err != nil {
		log.Println(err.Error())
		return []byte{}
	}

	end, err := strconv.Atoi(cmd.Args[2])
	if err != nil {
		log.Println(err.Error())
		return []byte{}
	}

	members := h.s.KVStore.ZRange(key, start, end)
	return resp.EncodeArray(members)
}

func (h *ConnHandler) handleZCARD(cmd CMD) []byte {
	key := cmd.Args[0]
	length := h.s.KVStore.ZCard(key)
	return resp.EncodeInt(length)
}
