package resp

import "fmt"

func EncodeInt(num int) (res []byte) {
	res = fmt.Appendf(res, ":%d\r\n", num)
	return
}
