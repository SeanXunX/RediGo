package resp

import "strings"

func CmpStrNoCase(str1, str2 string) bool {
	return strings.ToUpper(str1) == strings.ToUpper(str2)
}
