package qnet

import (
	"bufio"
	"net"
	"runtime"
	"sync/atomic"

	"github.com/lnmq/qcore/qconfig"

	"time"
)

const defaultBufferSize = 16 * 1024

const (
	StateInit = iota
	StateDisconnected
	StateConnected
	StateSubscribed
	StateClosing
)

type TcpServer struct {
	listener net.Listener
	ConnectDataHandler
	MessageLooper

	clientIdSequence uint64
	connectMgr       *ConnectMgr
}

type ConnectDataHandler interface {
	ConnectDataHandle([][]byte, *TcpConnect) ([]byte, error)
}

type MessageLooper interface {
	MessageLoop(conn *TcpConnect)
}

func NewTcpServer(handler ConnectDataHandler, looper MessageLooper) *TcpServer {

	s := &TcpServer{
		ConnectDataHandler: handler,
		MessageLooper:      looper,
		connectMgr:         &ConnectMgr{},
	}

	var err error
	s.listener, err = net.Listen("tcp", qconfig.Q_Config.TCPAddress)
	if err != nil {

	}

	return s
}

func (server *TcpServer) Start() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				runtime.Gosched()
				continue
			}

			break
		} else {
			c := server.createConnect(conn)
			c.connectMgr = server.connectMgr
			server.connectMgr.AddConnect(c)

			go c.Start()
		}
	}
}

func (server *TcpServer) createConnect(conn net.Conn) *TcpConnect {
	addr, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
	id := atomic.AddUint64(&server.clientIdSequence, 1)
	c := &TcpConnect{
		Id:                 id,
		ConnectDataHandler: server,
		MessageLooper:      server,
		Conn:               conn,
		Reader:             bufio.NewReaderSize(conn, defaultBufferSize),
		Writer:             bufio.NewWriterSize(conn, defaultBufferSize),

		OutputBufferSize:    defaultBufferSize,
		OutputBufferTimeout: qconfig.Q_Config.OutputBufferTimeout,
		MsgTimeout:          qconfig.Q_Config.MsgTimeout,

		ReadyStateChan: make(chan int32, 1),
		ExitChan:       make(chan int32, 1),
		ConnectTime:    time.Now(),
		State:          StateInit,

		ClientId: addr,
		Hostname: addr,

		HeartbeatInterval: qconfig.Q_Config.ClientTimeout / 2,

		pubCounts: make(map[string]int64),
	}

	return c
}
