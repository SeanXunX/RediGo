package resp

import "fmt"

func EncodeInt(num int) (res []byte) {
	res = fmt.Appendf(res, ":%d\r\n", num)
	return
}

func EncodeBulkString(str string) (res []byte) {
	res = fmt.Appendf(res, "$%d\r\n%s\r\n", len(str), str)
	return
}

func EncodeArray(l []string) (res []byte) {
	res = fmt.Appendf(res, "*%d\r\n", len(l))
	for _, str := range l {
		res = append(res, EncodeBulkString(str)...)
	}
	return
}
