package qnet

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/binary"
	"net"
	"time"
)

type TcpConnect struct {
	net.Conn

	ConnectHandler

	TlsConn     *tls.Conn
	flateWriter *flate.Writer

	connectMgr *ConnectMgr

	Reader *bufio.Reader
	Writer *bufio.Writer

	OutputBufferSize    int32
	OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration

	MsgTimeout time.Duration

	Id        uint64
	UserAgent string

	ClientId string
	Hostname string
}

func (c *TcpConnect) Start() {
	go c.MessageLoop()

	for {
		if c.HeartbeatInterval > 0 {
			c.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
		}

		line, err := c.Reader.ReadSlice('\n')
		if err != nil {
			break
		}

		line = line[:len(line)-1]
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, []byte(" "))
		res, err := c.ConnectDataHandler(params)
		if res != nil {
			c.Send(res)
		}
	}

	c.Conn.Close()

	c.CloseHandler()
	c.connectMgr.RemoveClient(c.Id)
}

func (c *TcpConnect) Send(data []byte) (int, error) {
	buf := [4]byte{}
	size := uint32(len(data))
	total := 0

	binary.BigEndian.PutUint32(buf[:], size)
	n, err := c.Writer.Write(buf[:])
	total += n
	if err != nil {
		return total, nil
	}

	n, err = c.Writer.Write(data)
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}
