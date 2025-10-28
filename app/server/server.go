package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

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

	// // PSYNC
	// conn.Write(resp.EncodeArray([]string{"PSYNC", "?", "-1"}))
	// line, _ = reader.ReadString('\n')
	// log.Println("PSYNC response:", line)
	//
	// // 如果是 FULLRESYNC，则接下来是 RDB 文件
	// if strings.HasPrefix(line, "+FULLRESYNC") {
	// 	// 开始接收 RDB 二进制数据
	// 	s.ReceiveRDB(reader)
	// }

	// PSYNC
	conn.Write(resp.EncodeArray([]string{"PSYNC", "?", "-1"}))
	line, err = reader.ReadString('\n')
	if err != nil {
		log.Println("Error reading PSYNC response:", err)
		return
	}

	line = strings.TrimSpace(line)
	log.Println("PSYNC response:", line)

	// 解析 FULLRESYNC 响应
	if strings.HasPrefix(line, "+FULLRESYNC") {
		parts := strings.Fields(line)
		if len(parts) >= 3 {
			s.MasterReplId = parts[1]
			offset, _ := strconv.Atoi(parts[2])
			s.SlaveReplOffset = offset
			log.Printf("Master repl ID: %s, offset: %d\n", s.MasterReplId, offset)
		}

		// 接收 RDB 文件
		err = s.ReceiveRDB(reader)
		if err != nil {
			log.Println("Failed to receive RDB:", err)
			return
		}
	}
	// 完成后再启动 handler 处理后续命令
	time.Sleep(1000 * time.Millisecond) // 如果这样能 100% 成功，说明是竞态问题
	h := NewConnHandler(conn, s)
	go h.Handle(true)
}

//	func (s *Server) ReceiveRDB(reader *bufio.Reader) {
//		// RDB 文件头是 "REDIS"
//		header := make([]byte, 5)
//		_, err := io.ReadFull(reader, header)
//		if err != nil {
//			log.Println("Error reading RDB header:", err)
//			return
//		}
//		if string(header) != "REDIS" {
//			log.Println("Invalid RDB header:", string(header))
//			return
//		}
//
//		// 剩余部分直接读到文件或内存
//		f, _ := os.Create("dump.rdb")
//		defer f.Close()
//		io.Copy(f, reader)
//		log.Println("RDB file received.")
//	}
func (s *Server) ReceiveRDB(reader *bufio.Reader) error {
	// 1. 读取 bulk string 的长度标识 "$<length>\r\n"
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Println("Error reading RDB length prefix:", err)
		return err
	}

	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "$") {
		log.Printf("Invalid RDB format, expected $, got: %s\n", line)
		return fmt.Errorf("invalid RDB format: %s", line)
	}

	// 2. 解析 RDB 文件长度
	lengthStr := line[1:] // 去掉 "$"
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		log.Println("Error parsing RDB length:", err)
		return err
	}

	log.Printf("Receiving RDB file of %d bytes\n", length)

	// 3. 读取指定长度的 RDB 数据
	rdbData := make([]byte, length)
	_, err = io.ReadFull(reader, rdbData)
	if err != nil {
		log.Println("Error reading RDB data:", err)
		return err
	}

	// 4. 验证 RDB 头（前5个字节应该是 "REDIS"）
	if len(rdbData) < 5 || string(rdbData[0:5]) != "REDIS" {
		log.Printf("Invalid RDB header: %s\n", string(rdbData[0:min(5, len(rdbData))]))
		return fmt.Errorf("invalid RDB header")
	}

	// 5. 可选：保存到文件
	if s.Dir != "" && s.Dbfilename != "" {
		filepath := filepath.Join(s.Dir, s.Dbfilename)
		err = os.WriteFile(filepath, rdbData, 0644)
		if err != nil {
			log.Println("Error writing RDB file:", err)
		} else {
			log.Println("RDB file saved to:", filepath)
		}
	}

	// 6. 可选：解析 RDB 加载数据到 KVStore
	// s.LoadRDB(rdbData)

	log.Println("RDB file received successfully")
	return nil
}
