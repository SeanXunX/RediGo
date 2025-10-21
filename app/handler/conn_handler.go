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
}

type CMD struct {
	Command string
	Args    []string
}

func NewConnHandler(conn net.Conn, store *kv.KVStore) *ConnHandler {
	return &ConnHandler{
		conn:    conn,
		in:      make(chan CMD),
		kvStore: store,
	}
}

func (h *ConnHandler) close() {
	h.conn.Close()
}

func (h *ConnHandler) readCMD() {
	reader := bufio.NewReader(h.conn)
	for {
		parts, err := resp.DecodeArray(reader)
		if err == io.EOF {
			log.Println("Client closed connection")
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

func (h *ConnHandler) Handle() {
	defer h.close()

	go h.readCMD()

	for cmd := range h.in {
		switch strings.ToUpper(cmd.Command) {
		case "PING":
			fmt.Fprint(h.conn, "+PONG\r\n")
		case "ECHO":
			fmt.Fprintf(h.conn, "$%d\r\n%s\r\n", len(cmd.Args[0]), cmd.Args[0])
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
			fmt.Fprint(h.conn, "+OK\r\n")
		case "GET":
			key := cmd.Args[0]
			val := h.kvStore.Get(key)
			if val != nil {
				fmt.Fprintf(h.conn, "$%d\r\n%s\r\n", len(val.(string)), val.(string))
			} else {
				fmt.Fprint(h.conn, "$-1\r\n")
			}
		case "RPUSH":
			key := cmd.Args[0]
			value := cmd.Args[1:]
			length := h.kvStore.RPush(key, value)
			h.conn.Write(resp.EncodeInt(length))
		case "LPUSH":
			key := cmd.Args[0]
			value := cmd.Args[1:]
			length := h.kvStore.LPush(key, value)
			h.conn.Write(resp.EncodeInt(length))
		case "LRANGE":
			key := cmd.Args[0]
			start, _ := strconv.Atoi(cmd.Args[1])
			stop, _ := strconv.Atoi(cmd.Args[2])
			l := h.kvStore.LRange(key, start, stop)
			h.conn.Write(resp.EncodeArray(l))
		case "LLEN":
			key := cmd.Args[0]
			length := h.kvStore.LLen(key)
			h.conn.Write(resp.EncodeInt(length))
		case "LPOP":
			h.handleLPOP(cmd)
		case "BLPOP":
			h.handleBLPOP(cmd)
		case "TYPE":
			h.handleType(cmd)
		}
	}

}

func (h *ConnHandler) handleLPOP(cmd CMD) {
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
		h.conn.Write(resp.EncodeNullBulkString())
		return
	}

	switch v := elem.(type) {
	case string:
		h.conn.Write(resp.EncodeBulkString(v))
	case []string:
		h.conn.Write(resp.EncodeArray(v))
	}
}

func (h *ConnHandler) handleBLPOP(cmd CMD) {
	key := cmd.Args[0]
	seconds, ok := strconv.ParseFloat(cmd.Args[1], 64)
	if ok != nil {
		log.Printf("Unexpected string for float64 waiting time: %s\n", cmd.Args[1])
	}
	timeout := time.Duration(seconds * float64(time.Second))

	log.Printf("[Debug] Parsed key: %s and timeout %v", key, timeout)

	elem := h.kvStore.BLPop(key, timeout)
	if elem == nil {
		h.conn.Write(resp.EncodeNullArray())
		return
	}
	res := []string{key, elem.(string)}
	h.conn.Write(resp.EncodeArray(res))
}

func (h *ConnHandler) handleType(cmd CMD) {
	key := cmd.Args[0]
	t := h.kvStore.Type(key)
	h.conn.Write(resp.EncodeSimpleString(t))
}
