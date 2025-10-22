package resp

import (
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/app/kv"
)

func EncodeInt(num int) (res []byte) {
	res = fmt.Appendf(res, ":%d\r\n", num)
	return
}

func EncodeBulkString(str string) (res []byte) {
	res = fmt.Appendf(res, "$%d\r\n%s\r\n", len(str), str)
	return
}

func EncodeNullBulkString() (res []byte) {
	res = fmt.Append(res, "$-1\r\n")
	return
}

func EncodeArray(l []string) (res []byte) {
	res = fmt.Appendf(res, "*%d\r\n", len(l))
	for _, str := range l {
		res = append(res, EncodeBulkString(str)...)
	}
	return
}

func EncodeNullArray() (res []byte) {
	res = fmt.Append(res, "*-1\r\n")
	return
}

func EncodeSimpleString(str string) (res []byte) {
	res = fmt.Appendf(res, "+%s\r\n", str)
	return
}

func EncodeSimpleError(str string) (res []byte) {
	res = fmt.Appendf(res, "-ERR %s\r\n", str)
	return
}

func EncodeXRangeRes(entries []kv.StreamEntry) (res []byte) {
	res = fmt.Appendf(res, "*%d\r\n", len(entries))
	for _, entry := range entries {
		// id
		res = fmt.Append(res, "*2\r\n")
		idStr := fmt.Sprintf("%d-%d", entry.ID.Ms, entry.ID.Seq)
		res = append(res, EncodeBulkString(idStr)...)

		// entry data
		res = fmt.Appendf(res, "*%d\r\n", len(entry.Data)*2)
		for k, v := range entry.Data {
			res = append(res, EncodeBulkString(k)...)
			res = append(res, EncodeBulkString(v)...)
		}
	}
	return
}
