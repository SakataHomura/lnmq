package qnet

import (
	"bufio"
	"net"
	"runtime"
	"sync/atomic"

	"github.com/lnmq/qconfig"
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
	listener       net.Listener
	HandlerCreator func(*TcpConnect) ConnectHandler

	clientIdSequence uint64
	connectMgr       *ConnectMgr
}

type ConnectHandler interface {
	ConnectDataHandler([][]byte) ([]byte, error)
	MessageLoop()
	CloseHandler()
}

func NewTcpServer(f func(*TcpConnect) ConnectHandler) *TcpServer {
	s := &TcpServer{
		HandlerCreator: f,
		connectMgr:     &ConnectMgr{},
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
		Id:     id,
		Conn:   conn,
		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),

		OutputBufferSize:    defaultBufferSize,
		OutputBufferTimeout: qconfig.Q_Config.OutputBufferTimeout,
		MsgTimeout:          qconfig.Q_Config.MsgTimeout,

		ClientId: addr,
		Hostname: addr,

		HeartbeatInterval: qconfig.Q_Config.ClientTimeout / 2,
	}

	c.ConnectHandler = server.HandlerCreator(c)

	return c
}
