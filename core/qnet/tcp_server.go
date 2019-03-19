package qnet

import (
    "net"
    "runtime"
    "sync/atomic"
    "bufio"

    "../config"

    "time"
)

const defaultBufferSize = 16 * 1024

const (
    stateInit = iota
    stateDisconnected
    stateConnected
    stateSubscribed
    stateClosing
)

type TcpServer struct {
    listener net.Listener
    handler ConnectHandler

    clientIdSequence uint64
    connectMgr ConnectMgr
}

type ConnectHandler interface {
    Handle()
}

func (server *TcpServer) Create() {
    
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
            //go server.handler.Handle(conn)
            c := server.createConnect(conn)
            server.connectMgr.AddConnect(c)
        }
    }
}

func (server *TcpServer) createConnect(conn net.Conn) *TcpConnect {
    addr, _, _ := net.SplitHostPort(conn.RemoteAddr().String())
    id := atomic.AddUint64(&server.clientIdSequence, 1)
    c := &TcpConnect{
        Id:id,
        Conn:conn,
        Reader:bufio.NewReaderSize(conn, defaultBufferSize),
        Writer:bufio.NewWriterSize(conn, defaultBufferSize),

        OutputBufferSize:defaultBufferSize,
        OutputBufferTimeout:config.GlobalConfig.OutputBufferTimeout,
        MsgTimeout:config.GlobalConfig.MsgTimeout,

        ReadyStateChan:make(chan int32, 1),
        ExitChan:make(chan int32, 1),
        ConnectTime:time.Now(),
        State:stateInit,

        ClientId:addr,
        Hostname:addr,

        HeartbeatInterval:config.GlobalConfig.ClientTimeout / 2,

        pubCounts:make(map[string]int64),
    }

    //c.lenSlice = c.lenBuf[:]
    return c
}