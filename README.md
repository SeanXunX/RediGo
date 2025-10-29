# 🚀 RediGo

[![Docker Hub](https://img.shields.io/badge/Docker%20Hub-seanxunx%2Fredigo-blue?logo=docker)](https://hub.docker.com/r/seanxunx/redigo)

A feature-rich Redis server implementation in Go, built as part of the [CodeCrafters](https://codecrafters.io) Redis challenge. This project implements the Redis Serialization Protocol (RESP) and supports a wide range of Redis commands and features.

## 🎯 Quick Start

```bash
# Pull and run from Docker Hub
docker pull seanxunx/redigo:latest
docker run -p 6379:6379 seanxunx/redigo:latest

# Connect with redis-cli
redis-cli -h localhost -p 6379
```

## ✨ Features

### 🔑 Data Structures
- **Strings** - Basic key-value operations with expiration support
- **Lists** - LPUSH, RPUSH, LPOP, LRANGE, LLEN, and blocking operations (BLPOP)
- **Sorted Sets** - ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM
- **Streams** - XADD, XRANGE, XREAD for event streaming
- **Geospatial** - GEOADD, GEOPOS, GEODIST, GEOSEARCH for location-based queries

### 🔄 Advanced Features
- **Master-Slave Replication** - Full replication support with PSYNC
- **RDB Persistence** - Load and save data from/to RDB files
- **Transactions** - MULTI, EXEC, DISCARD for atomic operations
- **Pub/Sub** - SUBSCRIBE, PUBLISH, UNSUBSCRIBE for messaging
- **Blocking Operations** - BLPOP with timeout support
- **RESP Protocol** - Full Redis Serialization Protocol implementation

### 📡 Supported Commands

#### Basic Commands
- `PING` - Test server connectivity
- `ECHO` - Echo the given string
- `COMMAND` - Get command info
- `INFO` - Server information
- `CONFIG` - Configuration management
- `KEYS` - Find keys matching pattern
- `TYPE` - Determine key type

#### String Commands
- `SET` - Set key to value with optional expiration (PX, EX)
- `GET` - Get value of key
- `INCR` - Increment key value

#### List Commands
- `RPUSH` - Push to right of list
- `LPUSH` - Push to left of list
- `LPOP` - Pop from left of list
- `BLPOP` - Blocking pop from left of list
- `LRANGE` - Get range of elements
- `LLEN` - Get list length

#### Sorted Set Commands
- `ZADD` - Add member with score
- `ZRANK` - Get member rank
- `ZRANGE` - Get range by index
- `ZCARD` - Get set cardinality
- `ZSCORE` - Get member score
- `ZREM` - Remove members

#### Stream Commands
- `XADD` - Add entry to stream
- `XRANGE` - Query range of entries
- `XREAD` - Read from streams (with blocking support)

#### Geospatial Commands
- `GEOADD` - Add location coordinates
- `GEOPOS` - Get position of members
- `GEODIST` - Get distance between members
- `GEOSEARCH` - Search by radius or box

#### Pub/Sub Commands
- `SUBSCRIBE` - Subscribe to channels
- `PUBLISH` - Publish message to channel
- `UNSUBSCRIBE` - Unsubscribe from channels

#### Transaction Commands
- `MULTI` - Start transaction
- `EXEC` - Execute transaction
- `DISCARD` - Discard transaction

#### Replication Commands
- `REPLCONF` - Replication configuration
- `PSYNC` - Synchronize with master
- `WAIT` - Wait for replication acknowledgment

## 🏗️ Project Structure

```
.
├── app/
│   ├── main.go           # Entry point
│   ├── server/           # TCP server and connection handling
│   │   ├── server.go     # Server implementation
│   │   ├── conn_handler.go # Command execution
│   │   └── parser.go     # RDB file parser
│   ├── resp/             # RESP protocol encoder/decoder
│   │   ├── encoder.go
│   │   ├── decoder.go
│   │   └── utils.go
│   ├── kv/               # Key-value store implementations
│   │   ├── kv.go         # Main store
│   │   ├── string.go     # String operations
│   │   ├── list.go       # List operations
│   │   ├── zset.go       # Sorted set operations
│   │   ├── stream.go     # Stream operations
│   │   ├── geo.go        # Geospatial operations
│   │   └── transaction.go # Transaction support
│   └── geospatial/       # Geospatial utilities
│       ├── geohash.go    # Geohash encoding
│       └── distance.go   # Distance calculations
├── Dockerfile
├── docker-compose.yml
├── go.mod
└── go.sum
```

## 🚀 Getting Started

### Prerequisites
- Go 1.25.0 or higher
- Docker (optional)

### Running Locally

```bash
# Run as master on default port 6379
go run ./app/main.go

# Run on custom port
go run ./app/main.go -port 6380

# Run as replica
go run ./app/main.go -port 6380 -replicaof "localhost 6379"

# Run with RDB persistence
go run ./app/main.go -dir /tmp/redis -dbfilename dump.rdb
```

### 🐳 Running with Docker

#### Using Pre-built Image from Docker Hub

```bash
# Pull the latest image
docker pull seanxunx/redigo:latest

# Run as master
docker run -p 6379:6379 seanxunx/redigo:latest

# Run with custom port
docker run -p 6380:6379 seanxunx/redigo:latest

# Run with volume for RDB persistence
docker run -p 6379:6379 -v $(pwd)/data:/data \
  seanxunx/redigo:latest -dir /data -dbfilename dump.rdb

# Using Docker Compose
docker-compose up -d
```

#### Building from Source

```bash
# Build locally
docker build -t redigo .
docker run -p 6379:6379 redigo
```

### 🔌 Connecting to the Server

```bash
# Using redis-cli
redis-cli -h localhost -p 6379

# Or using telnet
telnet localhost 6379
```

## 🎯 Usage Examples

```bash
# String operations
SET mykey "Hello World" PX 10000
GET mykey
INCR counter

# List operations
RPUSH mylist "item1" "item2" "item3"
LRANGE mylist 0 -1
LPOP mylist

# Sorted set operations
ZADD leaderboard 100 "player1" 200 "player2"
ZRANGE leaderboard 0 -1 WITHSCORES
ZRANK leaderboard "player1"

# Stream operations
XADD mystream * field1 value1 field2 value2
XRANGE mystream - +
XREAD STREAMS mystream 0-0

# Geospatial operations
GEOADD locations 13.361389 38.115556 "Palermo"
GEOADD locations 15.087269 37.502669 "Catania"
GEODIST locations "Palermo" "Catania" km
GEOSEARCH locations FROMLONLAT 15 37 BYRADIUS 200 km

# Pub/Sub
SUBSCRIBE news
PUBLISH news "Breaking news!"

# Transactions
MULTI
SET key1 value1
SET key2 value2
EXEC
```

## 🔧 Configuration Options

| Flag | Description | Default |
|------|-------------|---------|
| `-port` | Server port | 6379 |
| `-replicaof` | Master server address (host port) | "" (master mode) |
| `-dir` | Directory for RDB file | "" |
| `-dbfilename` | RDB filename | "" |

## 🏛️ Architecture Highlights

- **Concurrent-Safe** - All operations use Go's sync primitives for thread safety
- **RESP Protocol** - Full implementation of Redis Serialization Protocol
- **Non-Blocking I/O** - Each connection handled in its own goroutine
- **Master-Slave Replication** - Command propagation with offset tracking
- **Blocking Operations** - Efficient blocking with Go channels and condition variables
- **RDB Persistence** - Binary format parsing with expiration support

## 📦 Docker Image

The project builds a minimal Docker image using multi-stage builds:
- **Builder stage**: Compiles the Go application
- **Runtime stage**: Uses `scratch` for minimal image size (~10MB)
- **Publicly Available**: [seanxunx/redigo](https://hub.docker.com/r/seanxunx/redigo) on Docker Hub

Pull the image:
```bash
docker pull seanxunx/redigo:latest
```

## 🛠️ Development

```bash
# Install dependencies
go mod download

# Build
go build -o redigo ./app/

# Run tests (if available)
go test ./...
```

## 🤝 Contributing

This is a learning project built for CodeCrafters. Feel free to fork and experiment!

## 📝 License

This project is open source and available for educational purposes.

## 🎓 Acknowledgments

Built as part of the [CodeCrafters Redis Challenge](https://codecrafters.io/challenges/redis) - a hands-on way to learn how Redis works by building your own Redis server.

---

Made with ❤️ and Go
