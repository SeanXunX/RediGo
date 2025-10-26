package server

import (
	"fmt"
	"log"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/kv"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Server struct {
	Host string
	Port int
	Role string

	MasterReplId     string
	MasterReplOffset int

	Replicaof string

	KVStore *kv.KVStore

	SlaveConns []net.Conn
}

func NewServer(host string, port int, role string, masterReplId string, masterReplOffset int, replicaof string) *Server {
	return &Server{
		Host:             host,
		Port:             port,
		Role:             role,
		MasterReplId:     masterReplId,
		MasterReplOffset: masterReplOffset,
		Replicaof:        replicaof,
		KVStore:          kv.NewKVStore(),
		SlaveConns:       []net.Conn{},
	}
}

func (s *Server) Run() {
	addr := fmt.Sprintf("%s:%d", s.Host, s.Port)

	l, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		return
	}

	if s.Role == "slave" {
		s.SendHandShake()
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			return
		}

		h := NewConnHandler(conn, s)
		go h.Handle(false)
	}

}

func (s *Server) SendHandShake() {
	parts := strings.Split(s.Replicaof, " ")
	if len(parts) != 2 {
		log.Println("Invalid master address: ", s.Replicaof)
		return
	}
	masterAddr := net.JoinHostPort(parts[0], parts[1])
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		log.Println("Failed to connect")
		return
	}
	buf := make([]byte, 1024)
	conn.Write(resp.EncodeArray([]string{"PING"}))
	conn.Read(buf)
	conn.Write(resp.EncodeArray([]string{"REPLCONF", "listening-port", fmt.Sprintf("%d", s.Port)}))
	conn.Read(buf)
	conn.Write(resp.EncodeArray([]string{"REPLCONF", "capa", "psync2"}))
	conn.Read(buf)
	conn.Write(resp.EncodeArray([]string{"PSYNC", "?", "-1"}))
	conn.Read(buf)
	conn.Read(buf)

	h := NewConnHandler(conn, s)
	go h.Handle(true)
}
