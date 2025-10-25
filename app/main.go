package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/kv"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type ServerInfo map[string]string

var serverInfo = make(ServerInfo)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	port := flag.Int("port", 6379, "server port")
	serverInfo["port"] = strconv.Itoa(*port)
	replicaof := flag.String("replicaof", "", "replication of")

	flag.Parse()

	addr := fmt.Sprintf("localhost:%d", *port)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	var role string = "master"
	if len(*replicaof) > 0 {
		role = "slave"
		handShake(*replicaof)
	}
	serverInfo["role"] = role
	serverInfo["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	serverInfo["master_repl_offset"] = "0"

	kvStore := kv.NewKVStore()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		h := handler.NewConnHandler(conn, kvStore, serverInfo)
		go h.Handle()
	}
}

func handShake(replicaof string) {
	parts := strings.Split(replicaof, " ")
	if len(parts) != 2 {
		log.Println("Invalid master address: ", replicaof)
		return
	}
	masterAddr := net.JoinHostPort(parts[0], parts[1])
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		log.Println("Failed to connect")
		return
	}
	defer conn.Close()
	buf := make([]byte, 1024)
	conn.Write(resp.EncodeArray([]string{"PING"}))
	conn.Read(buf)
	conn.Write(resp.EncodeArray([]string{"REPLCONF", "listening-port", serverInfo["port"]}))
	conn.Read(buf)
	conn.Write(resp.EncodeArray([]string{"REPLCONF", "capa", "psync2"}))
	conn.Read(buf)
	conn.Write(resp.EncodeArray([]string{"PSYNC", "?", "-1"}))
	conn.Read(buf)
}
