package qnet

import "sync"

type ConnectMgr struct {
	mutex    sync.RWMutex
	connects map[uint64]*TcpConnect
}

func (c *ConnectMgr) AddConnect(conn *TcpConnect) {
	c.mutex.Lock()

	c.connects[conn.Id] = conn

	c.mutex.Unlock()
}

func (c *ConnectMgr) RemoveClient(clientId uint64) {
	c.mutex.Lock()

	delete(c.connects, clientId)

	c.mutex.Unlock()
}
