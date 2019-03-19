package qnet

import (
	"bufio"
	"compress/flate"
	"crypto/tls"
	"net"
	"sync"
	"time"
)

type TcpConnect struct {
	net.Conn

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

func (c *TcpConnect) Start(handler ConnectHandler) {

}