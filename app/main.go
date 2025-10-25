package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/app/handler"
	"github.com/codecrafters-io/redis-starter-go/app/kv"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	port := flag.Int("port", 6379, "server port")
	replicaof := flag.String("replicaof", "master", "replication of")

	flag.Parse()

	addr := fmt.Sprintf("localhost:%d", *port)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	serverInfo := make(map[string]string)
	var role string = "master"
	if *replicaof != "master" {
		role = "slave"
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
