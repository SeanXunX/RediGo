package resp

import "strings"

func CmpStrNoCase(str1, str2 string) bool {
	return strings.EqualFold(str1, str2)
}
