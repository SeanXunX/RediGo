package resp

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"
)

func DecodeArray(reader *bufio.Reader) ([]string, int, error) {
	totalBytes := 0

	numberLine, err := reader.ReadString('\n')
	if err != nil {
		// log.Printf("Error reading numberLine: %s\n", err.Error())
		return nil, totalBytes, fmt.Errorf("invalid first line")
	}
	totalBytes += len(numberLine)

	if !strings.HasPrefix(numberLine, "*") {
		// log.Printf("Missing * as the first byte. Received: %s\n", numberLine)
		return nil, totalBytes, fmt.Errorf("invalid array prefix")
	}

	count, err := strconv.Atoi(strings.TrimSpace(numberLine[1:]))
	if err != nil {
		log.Println("Invalid array length. Error: ", err.Error())
		return nil, totalBytes, err
	}

	parts := make([]string, count)
	for i := range parts {
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err.Error())
			return nil, totalBytes, err
		}
		totalBytes += len(lenLine)

		if !strings.HasPrefix(lenLine, "$") {
			log.Println("Missing $ as the first byte. Received: ", lenLine)
			return nil, totalBytes, fmt.Errorf("invalid bulk string prefix")
		}

		_, err = strconv.Atoi(strings.TrimSpace(lenLine[1:]))
		if err != nil {
			log.Println(err.Error())
			return nil, totalBytes, err
		}

		str, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err.Error())
			return nil, totalBytes, err
		}
		totalBytes += len(str)

		parts[i] = strings.TrimSpace(str)
	}

	return parts, totalBytes, nil
}
