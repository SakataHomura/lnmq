package qutils

import "fmt"

func Byte2Int64(b []byte) (int64, error) {
	n := int64(0)
	for i:=0; i<len(b); i++  {
		var v byte
		switch {
		case '0' <= b[i] && b[i] <= 9:
			v = b[i] - '0'
		default:
			return 0, fmt.Errorf("data valid")
		}

		n *= 10
		n += int64(v)
	}

	return n, nil
}