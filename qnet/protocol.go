package qnet

import (
	"encoding/binary"
	"io"
)

var OkBytes = []byte("OK")
var HeartbeatBytes = []byte("_heartbeat_")

var lenBuf = [4]byte{}

func ReadLen(r io.Reader) (int32, error) {
	var tmp []byte = lenBuf[:]
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}

	return int32(binary.BigEndian.Uint32(tmp)), nil
}
