package qnet

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"net"
	"sync"
	"time"
    "encoding/binary"
)

type TcpConnect struct {
	net.Conn

    ConnectDataHandler
    MessageLooper

	ReadyCount    int64
	InFlightCount int64
	MessageCount  int64
	FinishCount   int64
	RequeueCount  int64

	pubCounts map[string]int64

	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	Id        uint64
	UserAgent string

	tlsConn     *tls.Conn
	flateWriter *flate.Writer

	Reader *bufio.Reader
	Writer  *bufio.Writer

	OutputBufferSize    int32
	OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration

	MsgTimeout time.Duration

	State       int32
	ConnectTime time.Time
	//Channel *Channel
	TopicName string
	ChannelName string
	ReadyStateChan chan int32
	ExitChan       chan int32

	ClientId string
	Hostname string

	SampleRate int32

	//IdentifyEventChan
	//SubEvenChan

	Tls int32
	//Snappy
	Deflate int32
}

func (c *TcpConnect) Start() {
	go c.MessageLoop(c)

	for  {
		if c.HeartbeatInterval > 0 {
			c.SetReadDeadline(time.Now().Add(c.HeartbeatInterval * 2))
		}

		line, err := c.Reader.ReadSlice('\n')
		if err != nil {
			break
		}

		line = line[:len(line) - 1]
		if len(line) > 0 && line[len(line) - 1] == '\r' {
			line = line [:len(line) - 1]
		}
		params := bytes.Split(line, []byte(" "))
        res, err := c.ConnectDataHandle(params, c)
        if res != nil {
            c.Send(res)
        }
	}
}

func (c *TcpConnect) Send (data []byte) (int, error) {
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

func (c *TcpConnect) UnPause() {

}

func (c *TcpConnect) Pause() {

}

func (c *TcpConnect) Close() error {
    return nil
}

func (c *TcpConnect) TimedOutMessage() {

}

func (c *TcpConnect) Empty() {

}