package rdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

func ParseKeys(filePath string) ([]string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	header := make([]byte, 9)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}
	log.Printf("RDB Version: %s\n", string(header[5:]))

	keys := []string{}

	for {
		opcode, err := readByte(f)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		log.Printf("opcode is %X \n", opcode)

		switch opcode {
		case 0xFD: // expire in seconds
			if _, err := readUint32(f); err != nil {
				return keys, err
			}
			valueType, _ := readByte(f)
			key, err := readString(f)
			if err != nil {
				return keys, err
			}
			readValue(f, valueType)
			keys = append(keys, key)
		case 0xFC: // expire in milliseconds
			if _, err := readUint64(f); err != nil {
				return keys, err
			}
			valueType, _ := readByte(f)
			key, err := readString(f)
			if err != nil {
				return keys, err
			}
			readValue(f, valueType)
			keys = append(keys, key)
		case 0xFE: // SELECTDB - read db number and continue to next database
			dbNum, err := readByte(f)
			if err != nil {
				return keys, err
			}
			log.Printf("Switched to database: %d\n", dbNum)
			// Continue reading keys from this database
		case 0xFA: // AUX fields
			if _, err := readString(f); err != nil {
				return keys, err
			}
			if _, err := readString(f); err != nil {
				return keys, err
			}
		case 0xFB:
			if _, _, err := readLength(f); err != nil {
				return keys, err
			}
			if _, _, err := readLength(f); err != nil {
				return keys, err
			}

		case 0xFF: // End of RDB file
			// Skip 8-byte CRC64 checksum
			var checksum [8]byte
			io.ReadFull(f, checksum[:])
			log.Println("End of RDB file")
			return keys, nil
		default:
			// Regular key-value without expiry
			valueType := opcode
			key, err := readString(f)
			if err != nil {
				return keys, err
			}
			readValue(f, valueType)
			keys = append(keys, key)
		}
	}

	return keys, nil
}

func readByte(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

func readUint32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func readUint64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}

func readLength(r io.Reader) (uint64, bool, error) {
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		return 0, false, err
	}
	flag := (first[0] & 0xC0) >> 6
	switch flag {
	case 0:
		return uint64(first[0] & 0x3F), false, nil
	case 1:
		var next [1]byte
		if _, err := io.ReadFull(r, next[:]); err != nil {
			return 0, false, err
		}
		return uint64(first[0]&0x3F)<<8 | uint64(next[0]), false, nil
	case 2:
		var next [4]byte
		if _, err := io.ReadFull(r, next[:]); err != nil {
			return 0, false, err
		}
		return uint64(binary.BigEndian.Uint32(next[:])), false, nil
	case 3:
		// Special encoding - return the remaining 6 bits and a flag
		return uint64(first[0] & 0x3F), true, nil
	default:
		return 0, false, fmt.Errorf("invalid length encoding flag: %d", flag)
	}
}

func readString(r io.Reader) (string, error) {
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		return "", err
	}

	flag := (first[0] & 0xC0) >> 6

	// Handle special encoding (flag == 3)
	if flag == 3 {
		special := first[0] & 0x3F
		switch special {
		case 0: // 8-bit integer
			var val [1]byte
			if _, err := io.ReadFull(r, val[:]); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", val[0]), nil
		case 1: // 16-bit integer
			var val [2]byte
			if _, err := io.ReadFull(r, val[:]); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", binary.LittleEndian.Uint16(val[:])), nil
		case 2: // 32-bit integer
			var val [4]byte
			if _, err := io.ReadFull(r, val[:]); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", binary.LittleEndian.Uint32(val[:])), nil
		case 3: // LZF compressed string
			return "", fmt.Errorf("LZF compressed strings not supported")
		default:
			return "", fmt.Errorf("unknown special string encoding: %d", special)
		}
	}

	// Normal length-prefixed string
	var length uint64
	switch flag {
	case 0:
		length = uint64(first[0] & 0x3F)
	case 1:
		var next [1]byte
		if _, err := io.ReadFull(r, next[:]); err != nil {
			return "", err
		}
		length = uint64(first[0]&0x3F)<<8 | uint64(next[0])
	case 2:
		var next [4]byte
		if _, err := io.ReadFull(r, next[:]); err != nil {
			return "", err
		}
		length = uint64(binary.BigEndian.Uint32(next[:]))
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readValue(r io.Reader, valueType byte) (string, error) {
	switch valueType {
	case 0: // string - use readString to handle special encodings
		val, err := readString(r)
		if err != nil {
			return "", err
		} else {
			return val, nil
		}
	default:
		// Unknown types - skip safely
		return "", nil
	}
}
