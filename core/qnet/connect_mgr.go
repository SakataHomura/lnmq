package qnet

import "sync"

type ConnectMgr struct {
	lock     sync.RWMutex
	connects map[uint64]*TcpConnect
}

func (c *ConnectMgr) AddConnect(conn *TcpConnect) {
	c.lock.Lock()

	c.connects[conn.Id] = conn

	c.lock.Unlock()
}
