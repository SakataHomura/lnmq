package qnet

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	"encoding/binary"
	"github.com/lnmq/qcore/qauth"
	"github.com/lnmq/qcore/qconfig"
	"github.com/lnmq/qcore/qcore"
	"net"
	"sync"
	"sync/atomic"
	"time"
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

	writeMutex sync.RWMutex
	metaMutex  sync.RWMutex

	Id        uint64
	UserAgent string

	tlsConn     *tls.Conn
	flateWriter *flate.Writer

	connectMgr *ConnectMgr

	Reader *bufio.Reader
	Writer *bufio.Writer

	OutputBufferSize    int32
	OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration

	MsgTimeout time.Duration

	State       int32
	ConnectTime time.Time
	//Channel *Channel
	Channel        interface{}
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

	AuthSecret string
	AuthState  *qauth.AuthState
}

func (c *TcpConnect) Start() {
	go c.MessageLoop(c)

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
		res, err := c.ConnectDataHandle(params, c)
		if res != nil {
			c.Send(res)
		}
	}

	c.Conn.Close()
	close(c.ExitChan)

	channel := c.Channel
	if channel != nil {
		chanPtr := channel.(*qcore.Channel)
		chanPtr.RemoveClient(c.Id)
	}

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

func (c *TcpConnect) UnPause() {

}

func (c *TcpConnect) Pause() {

}

func (c *TcpConnect) Close() {
	c.SetReadyCount(0)
	c.UpdateReadyState(0)

	atomic.StoreInt32(&c.State, StateClosing)
}

func (c *TcpConnect) TimedOutMessage() {

}

func (c *TcpConnect) Empty() {

}

func (c *TcpConnect) UpdateReadyState(state int32) {
	c.ReadyStateChan <- state
}

func (c *TcpConnect) IsReadyForMessages() bool {
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}

	return true
}

func (c *TcpConnect) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddInt64(&c.MessageCount, 1)
}

func (c *TcpConnect) PublishedMessage(topic string, count int64) {
	c.metaMutex.Lock()
	c.pubCounts[topic] += count
	c.metaMutex.Unlock()
}

func (c *TcpConnect) TimeoutMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)

}

func (c *TcpConnect) RequeuedMessage() {
	atomic.AddInt64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
}

func (c *TcpConnect) FinishedMessage() {
	atomic.AddInt64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
}

func (c *TcpConnect) SetReadyCount(count int64) {
	atomic.StoreInt64(&c.ReadyCount, count)
}

func (c *TcpConnect) IsAuthEnabled() bool {
	return len(qconfig.Q_Config.AuthHttpAddresses) != 0
}

func (c *TcpConnect) HasAuth() bool {
	if c.AuthState != nil {
		return len(c.AuthState.Auths) != 0
	}

	return false
}

func (c *TcpConnect) IsAuthd(topic, channel string) (bool, error) {
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

func (c *TcpConnect) QueryAuthd() error {
	remoteIp, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		return err
	}

	tlsEnabled := atomic.LoadInt32(&c.Tls) == 1
	commonName := ""
	if tlsEnabled {
		tlsConnState := c.tlsConn.ConnectionState()
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

func (c *TcpConnect) Auth(secret string) error {
	c.AuthSecret = secret
	return c.QueryAuthd()
}
