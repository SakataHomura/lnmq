package qmqd

import (
	"github.com/lnmq/qauth"
	"github.com/lnmq/qconfig"
	"github.com/lnmq/qcore"
	"github.com/lnmq/qnet"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type TcpClient struct {
	tcpConn  *qnet.TcpConnect
	protocol *Protocol

	ReadyCount    int64
	InFlightCount int64
	MessageCount  int64
	FinishCount   int64
	RequeueCount  int64

	pubCounts map[string]int64

	writeMutex sync.RWMutex
	metaMutex  sync.RWMutex

	State       int32
	ConnectTime time.Time
	//Channel *Channel
	Channel        interface{}
	ReadyStateChan chan int32
	ExitChan       chan int32

	SampleRate int32

	//IdentifyEventChan
	//SubEvenChan

	Tls int32
	//Snappy
	Deflate int32

	AuthSecret string
	AuthState  *qauth.AuthState
}

func createTcpClient(conn *qnet.TcpConnect) qnet.ConnectHandler {
	c := &TcpClient{
		tcpConn: conn,

		ReadyStateChan: make(chan int32, 1),
		ExitChan:       make(chan int32, 1),
		ConnectTime:    time.Now(),
		State:          qnet.StateInit,

		pubCounts: make(map[string]int64),
	}
	c.protocol = NewProtocol(Q_Server)

	return c
}

func (c *TcpClient) ConnectDataHandler(params [][]byte) ([]byte, error) {
	return c.protocol.ConnectDataHandler(params, c)
}

func (c *TcpClient) MessageLoop() {
	c.protocol.MessageLoop(c)
}

func (c *TcpClient) CloseHandler() {
	close(c.ExitChan)

	channel := c.Channel
	if channel != nil {
		chanPtr := channel.(*qcore.Channel)
		chanPtr.RemoveClient(c.tcpConn.Id)
	}
}

func (c *TcpClient) UpdateReadyState(state int32) {
	c.ReadyStateChan <- state
}

func (c *TcpClient) IsReadyForMessages() bool {
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}

	return true
}

func (c *TcpClient) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddInt64(&c.MessageCount, 1)
}

func (c *TcpClient) PublishedMessage(topic string, count int64) {
	c.metaMutex.Lock()
	c.pubCounts[topic] += count
	c.metaMutex.Unlock()
}

func (c *TcpClient) TimeoutMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)

}

func (c *TcpClient) RequeuedMessage() {
	atomic.AddInt64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
}

func (c *TcpClient) FinishedMessage() {
	atomic.AddInt64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
}

func (c *TcpClient) SetReadyCount(count int64) {
	atomic.StoreInt64(&c.ReadyCount, count)
}

func (c *TcpClient) IsAuthEnabled() bool {
	return len(qconfig.Q_Config.AuthHttpAddresses) != 0
}

func (c *TcpClient) HasAuth() bool {
	if c.AuthState != nil {
		return len(c.AuthState.Auths) != 0
	}

	return false
}

func (c *TcpClient) IsAuthd(topic, channel string) (bool, error) {
	if c.AuthState == nil {
		return false, nil
	}

	if c.AuthState.IsExpired() {
		err := c.QueryAuthd()
		if err != nil {
			return false, err
		}
	}

	if c.AuthState.IsAllowed(topic, channel) {
		return true, nil
	}

	return false, nil
}

func (c *TcpClient) QueryAuthd() error {
	remoteIp, _, err := net.SplitHostPort(c.tcpConn.RemoteAddr().String())
	if err != nil {
		return err
	}

	tlsEnabled := atomic.LoadInt32(&c.Tls) == 1
	commonName := ""
	if tlsEnabled {
		tlsConnState := c.tcpConn.TlsConn.ConnectionState()
		if len(tlsConnState.PeerCertificates) > 0 {
			commonName = tlsConnState.PeerCertificates[0].Subject.CommonName
		}
	}

	authState, err := qauth.QueryAnyAuthd(qconfig.Q_Config.AuthHttpAddresses,
		remoteIp, tlsEnabled, commonName, c.AuthSecret,
		qconfig.Q_Config.HttpClientConnectTimeout, qconfig.Q_Config.HttpClientRequestTimeout)
	if err != nil {
		return err
	}

	c.AuthState = authState

	return nil
}

func (c *TcpClient) Auth(secret string) error {
	c.AuthSecret = secret
	return c.QueryAuthd()
}

func (c *TcpClient) TimedOutMessage() {

}

func (c *TcpClient) Empty() {

}

func (c *TcpClient) UnPause() {

}

func (c *TcpClient) Pause() {

}

func (c *TcpClient) Close() {
	c.SetReadyCount(0)
	c.UpdateReadyState(0)

	atomic.StoreInt32(&c.State, qnet.StateClosing)
}
