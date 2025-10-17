package resp

import (
	"bufio"
	"io"
	"log"
	"strconv"
	"strings"
)

func DecodeArray(reader *bufio.Reader) ([]string, error) {
	numberLine, err := reader.ReadString('\n')
	if err == io.EOF {
		log.Println("Client closed connection")
		return nil, io.EOF
	}
	if err != nil {
		log.Printf("Error reading numberLine: %s\n", err.Error())
		return nil, err
	}
	if !strings.HasPrefix(numberLine, "*") {
		log.Printf("Missing * as the first byte. Received: %s\n", numberLine)
		return nil, err
	}

	count, err := strconv.Atoi(strings.TrimSpace(numberLine[1:]))
	if err != nil {
		log.Println("Invalid array length. Error: ", err.Error())
		return nil, err
	}

	var parts []string = make([]string, count)
	for i := range parts {
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}

		if !strings.HasPrefix(lenLine, "$") {
			log.Println("Missing $ as the first byte. Received: ", lenLine)
			return nil, err
		}

		_, err = strconv.Atoi(strings.TrimSpace(lenLine[1:]))
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}

		str, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}

		parts[i] = strings.TrimSpace(str)
	}
	return parts, nil
}
