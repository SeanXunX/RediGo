package handler

import (
	"bufio"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/kv"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type valueWithExpireTime struct {
	val       string
	createdAt time.Time
	px        int
}

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
		}
	}

}

func (h *ConnHandler) readCMD() {
	reader := bufio.NewReader(h.conn)
	for {
		numberLine, err := reader.ReadString('\n')
		if err == io.EOF {
			log.Println("Client closed connection")
			return
		}
		if err != nil {
			log.Printf("Error reading numberLine: %s\n", err.Error())
			continue
		}
		if !strings.HasPrefix(numberLine, "*") {
			log.Printf("Missing * as the first byte. Received: %s\n", numberLine)
			continue
		}

		count, err := strconv.Atoi(strings.TrimSpace(numberLine[1:]))
		if err != nil {
			log.Println("Invalid array length. Error: ", err.Error())
			continue
		}

		var parts []string = make([]string, count)
		for i := range parts {
			lenLine, err := reader.ReadString('\n')
			if err != nil {
				log.Println(err.Error())
				continue
			}

			if !strings.HasPrefix(lenLine, "$") {
				log.Println("Missing $ as the first byte. Received: ", lenLine)
				continue
			}

			_, err = strconv.Atoi(strings.TrimSpace(lenLine[1:]))
			if err != nil {
				log.Println(err.Error())
				continue
			}

			str, err := reader.ReadString('\n')
			if err != nil {
				log.Println(err.Error())
				continue
			}

			parts[i] = strings.TrimSpace(str)
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
