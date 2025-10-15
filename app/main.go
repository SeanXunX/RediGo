package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func parseRespArray(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("Invalid array start: %s", line)
	}

	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, fmt.Errorf("Invalid array length: %v", err)
	}

	var parts []string = make([]string, count)
	for i := range parts {
		// string elem
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(lenLine, "$") {
			return nil, fmt.Errorf("Invalid bulk string header: %s", lenLine)
		}

		_, err = strconv.Atoi(strings.TrimSpace(lenLine[1:]))
		if err != nil {
			return nil, err
		}

		str, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}

		parts[i] = strings.TrimSpace(str)

	}
	return parts, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		b, err := reader.Peek(1)
		if err == io.EOF {
			return
		}
		if err != nil {
			fmt.Println("Error peeking the first byte: ", err.Error())
			return
		}

		switch b[0] {
		case '*':
			// handle array
			cmd, err := parseRespArray(reader)
			if err == io.EOF {
				return
			}
			if err != nil {
				fmt.Println("Error parsing array", err.Error())
				return
			}

			if len(cmd) > 0 && strings.ToLower(cmd[0]) == "echo" {
				fmt.Fprintf(conn, "+%s\r\n", cmd[1])
			} else if strings.ToLower(cmd[0]) == "ping" {
				fmt.Fprintf(conn, "+PONG\r\n")
			}
		default:
			return
		}

	}
}
