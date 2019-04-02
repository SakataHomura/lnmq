package qmqd

import (
	"bytes"
	"fmt"
	"github.com/lnmq/qcore"
	"github.com/lnmq/qerror"
	"github.com/lnmq/qnet"
	"github.com/lnmq/qutils"
	"io"
	"sync/atomic"
	"time"
	"unsafe"
)

type Protocol struct {
	server DataServer
}

type DataServer interface {
	GetChannel(string, string) *qcore.Channel
	GetTopic(string) *qcore.Topic
}

func getMessageId(b []byte) (*qcore.MessageId, error) {
	if len(b) != qcore.MsgIdLength {
		return nil, fmt.Errorf("invalid msgid")
	}

	return (*qcore.MessageId)(unsafe.Pointer(&b[0])), nil
}

func readMpub(r io.Reader, maxMessageSize int32, maxBodySize int32) ([]*qcore.Message, error) {
	numMessages, err := qnet.ReadLen(r)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB failed to read message count")
	}

	maxMessages := (maxBodySize - 4) / 5
	if numMessages <= 0 || numMessages > maxMessages {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB invalid message count")
	}

	messages := make([]*qcore.Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := qnet.ReadLen(r)
		if err != nil {
			return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB invalid message size")
		}

		if messageSize <= 0 {
			return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB invalid message size")
		}

		if messageSize > maxMessageSize {
			return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB message too big")
		}

		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB failed to read message body")
		}

		messages = append(messages, qcore.NewMessage(msgBody))
	}

	return messages, nil
}

func NewProtocol(s DataServer) *Protocol {
	return &Protocol{
		server: s,
	}
}

func (p *Protocol) MessageLoop(client *TcpClient) {
	var messageChan chan *qcore.ChannelMsg
	var channel *qcore.Channel
	heartbeatTicker := time.NewTicker(client.tcpConn.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C

	isFlush := false
	isExit := false
	for {
		if isExit {
			break
		}

		if channel == nil || !client.IsReadyForMessages() {
			messageChan = nil

		} else if isFlush {
			if client.Channel != nil {
				channel = client.Channel.(*qcore.Channel)
				messageChan = channel.MemoryMsgChan
			}
		}

		select {
		case <-client.ReadyStateChan:
			isFlush = true
		case <-heartbeatChan:
			_, err := client.tcpConn.Send(qnet.HeartbeatBytes)
			if err != nil {
				//...
			}
		case msg := <-messageChan:
			buf := &bytes.Buffer{}
			_, err := msg.WriteTo(buf)
			if err == nil {
				_, err = client.tcpConn.Send(buf.Bytes())
			}
			if err != nil {

			}
		case <-client.ExitChan:
			isExit = true
		}
	}

	heartbeatTicker.Stop()
}

func (p *Protocol) ConnectDataHandler(params [][]byte, client *TcpClient) ([]byte, error) {
	var buf []byte
	var err error

	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		buf, err = p.fin(params, client)
	case bytes.Equal(params[0], []byte("RDY")):
		buf, err = p.rdy(params, client)
	case bytes.Equal(params[0], []byte("REQ")):
		buf, err = p.req(params, client)
	case bytes.Equal(params[0], []byte("PUB")):
		buf, err = p.pub(params, client)
	case bytes.Equal(params[0], []byte("MPUB")):
		buf, err = p.mpub(params, client)
	case bytes.Equal(params[0], []byte("DPUB")):
		buf, err = p.dpub(params, client)
	case bytes.Equal(params[0], []byte("NOP")):
		buf, err = p.nop(params, client)
	case bytes.Equal(params[0], []byte("TOUCH")):
		buf, err = p.touch(params, client)
	case bytes.Equal(params[0], []byte("SUB")):
		buf, err = p.sub(params, client)
	case bytes.Equal(params[0], []byte("CLS")):
		buf, err = p.cls(params, client)
	case bytes.Equal(params[0], []byte("AUTH")):
		buf, err = p.auth(params, client)
	}

	return buf, err
}

func (p *Protocol) pub(params [][]byte, client *TcpClient) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "PUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB topic name is not valid")
	}

	bodyLen, err := qnet.ReadLen(client.tcpConn.Reader)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB failed to read message body size")
	}

	if bodyLen <= 0 || bodyLen > Q_Config.MaxMessageSize {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB message size is not valid")
	}

	msgBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.tcpConn.Reader, msgBody)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB failed to read message")
	}

	topic := p.server.GetTopic(topicName)
	msg := qcore.NewMessage(msgBody)
	topic.PutMessage(msg)

	client.PublishedMessage(topicName, 1)

	return qnet.OkBytes, nil
}

func (p *Protocol) dpub(params [][]byte, client *TcpClient) ([]byte, error) {
	if len(params) < 3 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "DPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "DPUB topic name is not valid")
	}

	timeoutMs, err := qutils.Byte2Int64(params[2])
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "DPUB failed to parse timeout")
	}

	timeoutDu := time.Duration(timeoutMs) * time.Millisecond
	if timeoutDu < 0 || timeoutDu > Q_Config.MaxReqTimeout {
		return nil, qerror.MakeError(qerror.INVALID, "DPUB timeout out of range")
	}

	bodyLen, err := qnet.ReadLen(client.tcpConn.Reader)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "DPUB failed to read message body size")
	}

	if bodyLen <= 0 || bodyLen > Q_Config.MaxMessageSize {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "DPUB message size is not valid")
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.tcpConn.Reader, messageBody)
	if err != nil {
		return nil, err
	}

	topic := p.server.GetTopic(topicName)
	msg := qcore.NewMessage(messageBody)
	msg.Deferred = timeoutDu
	topic.PutMessage(msg)

	client.PublishedMessage(topicName, 1)

	return qnet.OkBytes, nil
}

func (p *Protocol) mpub(params [][]byte, client *TcpClient) ([]byte, error) {
	if len(params) < 2 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "MPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB topic name is not valid")
	}

	bodyLen, err := qnet.ReadLen(client.tcpConn.Reader)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB failed to read message body size")
	}

	if bodyLen <= 0 || bodyLen > Q_Config.MaxMessageSize {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB message size is not valid")
	}

	messages, err := readMpub(client.tcpConn.Reader, Q_Config.MaxMessageSize, Q_Config.MaxBodySize)
	if err != nil {
		return nil, err
	}

	topic := p.server.GetTopic(topicName)
	topic.PutMessages(messages)

	client.PublishedMessage(topicName, int64(len(messages)))

	return qnet.OkBytes, nil
}

func (p *Protocol) sub(params [][]byte, client *TcpClient) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != qnet.StateInit {
		return nil, qerror.MakeError(qerror.INVALID, "SUB connect state invalid")
	}

	if len(params) < 3 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "SUB topic name is not valid")
	}

	chanName := string(params[2])
	if !qcore.IsValidName(chanName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "SUB channel name is not valid")
	}

	channel := p.server.GetChannel(topicName, chanName)
	err := channel.AddClient(client.tcpConn.Id, client)
	if err != nil {
		return nil, err
	}

	atomic.StoreInt32(&client.State, qnet.StateSubscribed)
	client.UpdateReadyState(1)

	client.Channel = channel

	return qnet.OkBytes, nil
}

func (p *Protocol) req(params [][]byte, client *TcpClient) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != qnet.StateSubscribed && state != qnet.StateClosing {
		return nil, qerror.MakeError(qerror.INVALID, "REQ connect state invalid")
	}

	if len(params) < 3 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "REQ insufficient number of parameters")
	}

	id, err := getMessageId(params[1])
	if err != nil {
		return nil, err
	}

	timeUs, err := qutils.Byte2Int64(params[2])
	if err != nil {
		return nil, err
	}

	timeDu := time.Duration(timeUs) * time.Millisecond

	maxReqTimeout := Q_Config.MaxReqTimeout

	if timeDu < 0 {
		timeDu = 0
	} else if timeDu > maxReqTimeout {
		timeDu = maxReqTimeout
	}

	channel := client.Channel.(*qcore.Channel)
	err = channel.RequeueMessage(client.tcpConn.Id, *id, timeDu)
	if err != nil {
		return nil, err
	}

	client.RequeuedMessage()

	return qnet.OkBytes, nil
}

func (p *Protocol) fin(params [][]byte, client *TcpClient) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != qnet.StateSubscribed && state != qnet.StateClosing {
		return nil, qerror.MakeError(qerror.INVALID, "FIN connect state invalid")
	}

	if len(params) < 2 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "FIN insufficient number of parameters")
	}

	id, err := getMessageId(params[1])
	if err != nil {
		return nil, err
	}

	channel := client.Channel.(*qcore.Channel)
	err = channel.FinishMessage(client.tcpConn.Id, *id)
	if err != nil {
		return nil, err
	}

	client.FinishedMessage()

	return nil, nil
}

func (p *Protocol) rdy(params [][]byte, client *TcpClient) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == qnet.StateClosing {
		return nil, nil
	}

	if state != qnet.StateSubscribed {
		return nil, qerror.MakeError(qerror.INVALID, "RDY connect state invalid")
	}

	count := int64(1)
	if len(params) > 1 {
		n, _ := qutils.Byte2Int64(params[1])
		if n > 0 {
			count = int64(n)
		}
	}

	client.SetReadyCount(count)
	client.UpdateReadyState(1)

	return nil, nil
}

func (p *Protocol) nop(params [][]byte, client *TcpClient) ([]byte, error) {
	return nil, nil
}

func (p *Protocol) touch(params [][]byte, client *TcpClient) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != qnet.StateSubscribed && state != qnet.StateClosing {
		return nil, qerror.MakeError(qerror.INVALID, "TOUCH connect state invalid")
	}

	if len(params) < 2 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "TOUCH insufficient number of parameters")
	}

	id, err := getMessageId(params[1])
	if err != nil {
		return nil, err
	}

	channel := client.Channel.(*qcore.Channel)
	err = channel.TouchMessage(client.tcpConn.Id, *id, client.tcpConn.MsgTimeout)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (p *Protocol) auth(params [][]byte, client *TcpClient) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != qnet.StateInit {
		return nil, qerror.MakeError(qerror.INVALID, "AUTH connect state invalid")
	}

	if len(params) != 1 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "AUTH insufficient number of parameters")
	}

	bodyLen, err := qnet.ReadLen(client.tcpConn.Reader)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "AUTH failed to read message body size")
	}

	if bodyLen <= 0 || bodyLen > Q_Config.MaxMessageSize {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "AUTH message size is not valid")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.tcpConn.Reader, body)
	if err != nil {
		return nil, err
	}

	if client.HasAuth() {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "AUTH already set")
	}

	if !client.IsAuthEnabled() {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "AUTH disabled")
	}

	if err := client.Auth(string(body)); err != nil {
		return nil, qerror.MakeError(qerror.AUTH_FAILED, "AUTH failed")
	}

	if !client.HasAuth() {
		return nil, qerror.MakeError(qerror.AUTH_UNAUTH, "AUTH no auth found")
	}

	return qnet.OkBytes, nil
}

func (p *Protocol) cls(params [][]byte, client *TcpClient) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != qnet.StateSubscribed {
		return nil, qerror.MakeError(qerror.INVALID, "CLS connect state invalid")
	}

	client.Close()

	return []byte("CLOSE_WAIT"), nil
}
