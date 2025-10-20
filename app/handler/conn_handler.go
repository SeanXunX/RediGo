package handler

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/kv"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type ConnHandler struct {
	conn net.Conn
	in   chan CMD
}

type CMD struct {
	Command string
	Args    []string
}

func NewConnHandler(conn net.Conn) *ConnHandler {
	return &ConnHandler{
		conn: conn,
		in:   make(chan CMD),
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
				kv.Set(key, value)
			} else if len(cmd.Args) == 4 {
				t, _ := strconv.Atoi(cmd.Args[3])
				if strings.ToUpper(cmd.Args[2]) == "EX" {
					t *= 1000
				}
				kv.SetExpire(key, value, t)
			}
			fmt.Fprint(h.conn, "+OK\r\n")
		case "GET":
			key := cmd.Args[0]
			val := kv.Get(key)
			if val != nil {
				fmt.Fprintf(h.conn, "$%d\r\n%s\r\n", len(val.(string)), val.(string))
			} else {
				fmt.Fprint(h.conn, "$-1\r\n")
			}
		case "RPUSH":
			key := cmd.Args[0]
			value := cmd.Args[1:]
			length := kv.RPush(key, value)
			h.conn.Write(resp.EncodeInt(length))
		case "LPUSH":
			key := cmd.Args[0]
			value := cmd.Args[1:]
			length := kv.LPush(key, value)
			h.conn.Write(resp.EncodeInt(length))
		case "LRANGE":
			key := cmd.Args[0]
			start, _ := strconv.Atoi(cmd.Args[1])
			stop, _ := strconv.Atoi(cmd.Args[2])
			l := kv.LRange(key, start, stop)
			h.conn.Write(resp.EncodeArray(l))
		case "LLEN":
			key := cmd.Args[0]
			length := kv.LLen(key)
			h.conn.Write(resp.EncodeInt(length))
		case "LPOP":
			h.handleLPOP(cmd)
		}
	}

}

func (h *ConnHandler) handleLPOP(cmd CMD) {
	key := cmd.Args[0]
	var elem any

	switch len(cmd.Args) {
	case 1:
		elem = kv.LPop(key)
	case 2:
		num, _ := strconv.Atoi(cmd.Args[1])
		elem = kv.LPopN(key, num)
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
