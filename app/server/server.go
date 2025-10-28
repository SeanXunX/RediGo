package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/kv"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

type Server struct {
	Host string
	Port int
	Role string

	MasterReplId string

	MasterOffsetMu   sync.RWMutex
	MasterReplOffset int // Write by multi clients (propagate) and self (getack). Read by self.

	Replicaof string

	SlaveReplOffset int // Only written by slave itself.

	KVStore *kv.KVStore // Concurrent safe. No need for mutex.

	SlaveMu    sync.RWMutex
	SlaveConns []net.Conn // Written by slaves. Read by self.

	ackMu  sync.RWMutex
	ackCnt int

	Dir        string
	Dbfilename string

	PubSub *PubSubManager
}

// Pub/Sub 管理器
type PubSubManager struct {
	mu sync.RWMutex

	// channel -> subscriber connections
	channels map[string]map[net.Conn]*Subscriber

	// // pattern -> subscriber connections
	// patterns map[string]map[net.Conn]*Subscriber

	// connection -> subscribed channels
	subscribers map[net.Conn]*Subscriber
}

type Subscriber struct {
	mu       sync.RWMutex
	Channels map[string]bool // 订阅的普通 channel
	// Patterns map[string]bool // 订阅的 pattern
	// WriteCh  chan []byte // 异步写入通道，避免阻塞发布者
}

func NewPubSubManager() *PubSubManager {
	return &PubSubManager{
		channels: make(map[string]map[net.Conn]*Subscriber),
		// patterns:    make(map[string]map[net.Conn]*Subscriber),
		subscribers: make(map[net.Conn]*Subscriber),
	}
}

func NewSubscriber() *Subscriber {
	return &Subscriber{
		Channels: make(map[string]bool),
	}
}

func NewServer(host string, port int, role string, masterReplId string, masterReplOffset int, replicaof string, dir, dbfilename string) *Server {
	server := &Server{
		Host:             host,
		Port:             port,
		Role:             role,
		MasterReplId:     masterReplId,
		MasterReplOffset: masterReplOffset,
		Replicaof:        replicaof,
		KVStore:          kv.NewKVStore(),
		SlaveConns:       []net.Conn{},
		Dir:              dir,
		Dbfilename:       dbfilename,
		PubSub:           NewPubSubManager(),
	}
	if dir != "" && dbfilename != "" {
		filepath := path.Join(dir, dbfilename)
		server.Parse(filepath)
	}
	return server
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
		log.Println("Invalid master address:", s.Replicaof)
		return
	}

	masterAddr := net.JoinHostPort(parts[0], parts[1])
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		log.Println("Failed to connect to master:", err)
		return
	}

	reader := bufio.NewReader(conn)

	// PING
	conn.Write(resp.EncodeArray([]string{"PING"}))
	line, _ := reader.ReadString('\n')
	log.Println("PING response:", line)

	// REPLCONF listening-port
	conn.Write(resp.EncodeArray([]string{"REPLCONF", "listening-port", fmt.Sprintf("%d", s.Port)}))
	line, _ = reader.ReadString('\n')
	log.Println("REPLCONF port response:", line)

	// REPLCONF capa psync2
	conn.Write(resp.EncodeArray([]string{"REPLCONF", "capa", "psync2"}))
	line, _ = reader.ReadString('\n')
	log.Println("REPLCONF capa response:", line)

	// PSYNC
	conn.Write(resp.EncodeArray([]string{"PSYNC", "?", "-1"}))
	line, _ = reader.ReadString('\n')
	log.Println("PSYNC response:", line)

	// 如果是 FULLRESYNC，则接下来是 RDB 文件
	if strings.HasPrefix(line, "+FULLRESYNC") {
		// 开始接收 RDB 二进制数据
		s.ReceiveRDB(reader)
	}

	// 完成后再启动 handler 处理后续命令
	h := NewConnHandler(conn, s)
	go h.Handle(true)
}

func (s *Server) ReceiveRDB(reader *bufio.Reader) {
	// RDB 文件头是 "REDIS"
	header := make([]byte, 5)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		log.Println("Error reading RDB header:", err)
		return
	}
	if string(header) != "REDIS" {
		log.Println("Invalid RDB header:", string(header))
		// return
	}

	// 剩余部分直接读到文件或内存
	f, _ := os.Create("dump.rdb")
	defer f.Close()
	io.Copy(f, reader)
	log.Println("RDB file received.")
}
