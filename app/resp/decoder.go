package resp

import (
	"bufio"
	"log"
	"strconv"
	"strings"
)

func DecodeArray(reader *bufio.Reader) ([]string, error) {
	log.Println("[debug] Into Decode Array 1")
	numberLine, err := reader.ReadString('\n')
	log.Println("[debug] Into Decode Array 2: numberLine = ", numberLine)
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
	log.Println("[debug] Into Decode Array 3: count = ", count)

	var parts []string = make([]string, count)
	for i := range parts {
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
		log.Println("[debug] Into Decode Array 4: lenLine = ", lenLine)

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
		log.Println("[debug] Into Decode Array 5: str = ", str)
	}
	log.Println("[debug] Into Decode Array 6: -----------")
	return parts, nil
}
