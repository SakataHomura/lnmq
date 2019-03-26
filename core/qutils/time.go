package qutils

import "time"

func GetTimeMillis() int64 {
	return time.Now().UnixNano() / 1000
}

func tilNextMillis(lastTimestamp int64) int64 {
	timestamp := GetTimeMillis()
	for timestamp <= lastTimestamp {
		timestamp = GetTimeMillis()
	}
	return timestamp
}
