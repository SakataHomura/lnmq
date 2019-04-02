package qlookup

import (
	"bytes"
	"encoding/json"
	"github.com/lnmq/qcore"
	"github.com/lnmq/qerror"
	"github.com/lnmq/qnet"
	"github.com/lnmq/qversion"
	"io"
	"os"
	"sync/atomic"
	"time"
)

type Protocol struct {
	server *LookupServer
}

func NewProtocol() *Protocol {
	return &Protocol{}
}

func (p *Protocol) MessageLoop(client *TcpClient) {

}

func (p *Protocol) ConnectDataHandler(params [][]byte, client *TcpClient) ([]byte, error) {
	var buf []byte
	var err error

	switch {
	case bytes.Equal(params[0], []byte("PING")):
		buf, err = p.ping(params, client)
	case bytes.Equal(params[0], []byte("IDENTIFY")):
		buf, err = p.identify(params, client)
	case bytes.Equal(params[0], []byte("REGISTER")):
		buf, err = p.register(params, client)
	case bytes.Equal(params[0], []byte("UNREGISTER")):
		buf, err = p.unregister(params, client)
	}

	return buf, err
}

func (p *Protocol) ping(params [][]byte, client *TcpClient) ([]byte, error) {
	if client.peerInfo != nil {
		now := time.Now()
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}

	return qnet.OkBytes, nil
}

func (p *Protocol) identify(params [][]byte, client *TcpClient) ([]byte, error) {
	var err error

	if client.peerInfo != nil {
		return nil, qerror.MakeError(qerror.INVALID, "IDENTIFY already set")
	}

	bodyLen, err := qnet.ReadLen(client.tcpConn.Reader)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "IDENTIFY failed to read message body size")
	}

	if bodyLen <= 0 || bodyLen > Q_Config.MaxMessageSize {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "IDENTIFY message size is not valid")
	}

	msgBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.tcpConn.Reader, msgBody)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "IDENTIFY failed to read message")
	}

	peerInfo := PeerInfo{id: client.tcpConn.RemoteAddr().String()}
	err = json.Unmarshal(msgBody, &peerInfo)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "IDENTIFY failed to unmarshal message")
	}

	peerInfo.RemoreAddress = client.tcpConn.RemoteAddr().String()

	if peerInfo.BroadcastAddress == "" || peerInfo.TcpPort == 0 || peerInfo.HttpPort == 0 || peerInfo.Version == "" {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "IDENTIFY missing fields")
	}

	peerInfo.lastUpdate = time.Now().UnixNano()

	client.peerInfo = &peerInfo
	p.server.AddProducer(LookupKey{"client", "", ""}, &Producer{peerInfo: &peerInfo})

	res := struct {
		TcpPort          int32  `json:"tcp_port"`
		HttpPort         int32  `json:"http_port"`
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
	}{}

	res.TcpPort = peerInfo.TcpPort
	res.HttpPort = peerInfo.HttpPort
	res.Version = qversion.Version
	res.Hostname, _ = os.Hostname()
	res.BroadcastAddress = Q_Config.BroadcastAddress

	response, err := json.Marshal(res)
	if err != nil {
		return qnet.OkBytes, nil
	}

	return response, nil
}

func (p *Protocol) register(params [][]byte, client *TcpClient) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, qerror.MakeError(qerror.INVALID, "REGISTER must identify")
	}

	topicName := string(params[1])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "REGISTER topic name is not valid")
	}

	chanName := ""
	if len(params) >= 2 {
		chanName = string(params[2])
		if chanName != "" && !qcore.IsValidName(chanName) {
			return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "REGISTER channel name is not valid")
		}
	}

	if chanName != "" {
		key := LookupKey{"channel", topicName, chanName}
		p.server.AddProducer(key, &Producer{peerInfo: client.peerInfo})
	}

	key := LookupKey{"topic", topicName, ""}
	p.server.AddProducer(key, &Producer{peerInfo: client.peerInfo})

	return qnet.OkBytes, nil
}

func (p *Protocol) unregister(params [][]byte, client *TcpClient) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, qerror.MakeError(qerror.INVALID, "UNREGISTER must identify")
	}

	topicName := string(params[1])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "UNREGISTER topic name is not valid")
	}

	chanName := ""
	if len(params) >= 3 {
		chanName = string(params[2])
		if chanName != "" && !qcore.IsValidName(chanName) {
			return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "REGISTER channel name is not valid")
		}
	}

	if chanName != "" {
		key := LookupKey{"channel", topicName, chanName}
		p.server.RemoveProducer(key, client.peerInfo.id)
	} else {
		keys := p.server.FindLookupKey("channel", topicName, "*")
		for _, k := range keys {
			p.server.RemoveProducer(k, client.peerInfo.id)
		}

		key := LookupKey{"topic", topicName, ""}
		p.server.RemoveProducer(key, client.peerInfo.id)
	}

	return qnet.OkBytes, nil
}
