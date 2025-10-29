package main

import (
	"flag"
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/server"
)

type ServerInfo map[string]string

var serverInfo = make(ServerInfo)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	host := "0.0.0.0"

	port := flag.Int("port", 6379, "server port")
	replicaof := flag.String("replicaof", "", "replication of")
	dir := flag.String("dir", "", "directory where RDB file is stored")
	dbfilename := flag.String("dbfilename", "", "the name of RDB file")

	flag.Parse()

	var role string = "master"
	if len(*replicaof) > 0 {
		role = "slave"
	}
	serverInfo["role"] = role
	master_replid := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	master_repl_offset := 0

	s := server.NewServer(
		host,
		*port,
		role,
		master_replid,
		master_repl_offset,
		*replicaof,
		*dir,
		*dbfilename,
	)

	s.Run()
}
