package qmqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/lnmq/qcore/qconfig"
	"github.com/lnmq/qcore/qcore"
	"github.com/lnmq/qcore/qerror"
	"github.com/lnmq/qcore/qnet"
	"github.com/lnmq/qcore/qutils"
	"io"
	"sync/atomic"
	"time"
	"unsafe"
)

var okBytes = []byte("OK")
var heartbeatBytes = []byte("_heartbeat_")

type Protocol struct {
	server DataServer
}

type DataServer interface {
	GetChannel(string, string) *qcore.Channel
	GetTopic(string) *qcore.Topic
}


var lenBuf = [4]byte{}

func getMessageId(b []byte) (*qcore.MessageId, error) {
	if len(b) != qcore.MsgIdLength {
		return nil, fmt.Errorf("invalid msgid")
	}

	return (*qcore.MessageId)(unsafe.Pointer(&b[0])), nil
}



func readLen(r io.Reader) (int32, error) {
	var tmp []byte = lenBuf[:]
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}

	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func readMpub(r io.Reader, maxMessageSize int32, maxBodySize int32) ([]*qcore.Message, error) {
	numMessages, err := readLen(r)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB failed to read message count")
	}

	maxMessages := (maxBodySize - 4) / 5
	if numMessages <= 0 || numMessages > maxMessages {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB invalid message count")
	}

	messages := make([]*qcore.Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := readLen(r)
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

func (p *Protocol) MessageLoop(conn *qnet.TcpConnect) {
	var messageChan chan *qcore.ChannelMsg
	var channel *qcore.Channel
	heartbeatTicker := time.NewTicker(conn.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C

	isFlush := false
	isExit := false
	for {
		if isExit {
			break
		}

		if channel == nil || !conn.IsReadyForMessages() {
			messageChan = nil

		} else if isFlush {
			if conn.Channel != nil {
				channel = conn.Channel.(*qcore.Channel)
				messageChan = channel.MemoryMsgChan
			}
		}

		select {
		case <-conn.ReadyStateChan:
			isFlush = true
		case <-heartbeatChan:
			_, err := conn.Send(heartbeatBytes)
			if err != nil {
				//...
			}
		case msg := <-messageChan:
			buf := &bytes.Buffer{}
			_, err := msg.WriteTo(buf)
			if err == nil {
				_, err = conn.Send(buf.Bytes())
			}
			if err != nil {

			}
		case <-conn.ExitChan:
			isExit = true
		}
	}

	heartbeatTicker.Stop()
}

func (p *Protocol) ConnectDataHandle(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	var buf []byte
	var err error

	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		buf, err = p.fin(params, connect)
	case bytes.Equal(params[0], []byte("RDY")):
		buf, err = p.rdy(params, connect)
	case bytes.Equal(params[0], []byte("REQ")):
		buf, err = p.req(params, connect)
	case bytes.Equal(params[0], []byte("PUB")):
		buf, err = p.pub(params, connect)
	case bytes.Equal(params[0], []byte("MPUB")):
		buf, err = p.mpub(params, connect)
	case bytes.Equal(params[0], []byte("DPUB")):
		buf, err = p.dpub(params, connect)
	case bytes.Equal(params[0], []byte("NOP")):
		buf, err = p.nop(params, connect)
	case bytes.Equal(params[0], []byte("TOUCH")):
		buf, err = p.touch(params, connect)
	case bytes.Equal(params[0], []byte("SUB")):
		buf, err = p.sub(params, connect)
	case bytes.Equal(params[0], []byte("CLS")):
		buf, err = p.cls(params, connect)
	case bytes.Equal(params[0], []byte("AUTH")):
		buf, err = p.auth(params, connect)
	}

	return buf, err
}

func (p *Protocol) pub(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "PUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB topic name is not valid")
	}

	bodyLen, err := readLen(connect.Reader)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB failed to read message body size")
	}

	if bodyLen <= 0 || bodyLen > qconfig.Q_Config.MaxMessageSize {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB message size is not valid")
	}

	msgBody := make([]byte, bodyLen)
	_, err = io.ReadFull(connect.Reader, msgBody)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB failed to read message")
	}

	topic := p.server.GetTopic(topicName)
	msg := qcore.NewMessage(msgBody)
	topic.PutMessage(msg)

	connect.PublishedMessage(topicName, 1)

	return okBytes, nil
}

func (p *Protocol) dpub(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	if len(params) < 3 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "DPUB insufficient number of parameters")
	}

	topicName := string(params[0])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "DPUB topic name is not valid")
	}

	timeoutMs, err := qutils.Byte2Int64(params[2])
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "DPUB failed to parse timeout")
	}

	timeoutDu := time.Duration(timeoutMs) * time.Millisecond
	if timeoutDu < 0 || timeoutDu > qconfig.Q_Config.MaxReqTimeout {
		return nil, qerror.MakeError(qerror.INVALID, "DPUB timeout out of range")
	}

	bodyLen, err := readLen(connect.Reader)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "DPUB failed to read message body size")
	}

	if bodyLen <= 0 || bodyLen > qconfig.Q_Config.MaxMessageSize {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "DPUB message size is not valid")
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(connect.Reader, messageBody)
	if err != nil {
		return nil, err
	}

	topic := p.server.GetTopic(topicName)
	msg := qcore.NewMessage(messageBody)
	msg.Deferred = timeoutDu
	topic.PutMessage(msg)

	connect.PublishedMessage(topicName, 1)

	return okBytes, nil
}

func (p *Protocol) mpub(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	if len(params) < 2 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "MPUB insufficient number of parameters")
	}

	topicName := string(params[0])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB topic name is not valid")
	}

	bodyLen, err := readLen(connect.Reader)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB failed to read message body size")
	}

	if bodyLen <= 0 || bodyLen > qconfig.Q_Config.MaxMessageSize {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "MPUB message size is not valid")
	}

	messages, err := readMpub(connect.Reader, qconfig.Q_Config.MaxMessageSize, qconfig.Q_Config.MaxBodySize)
	if err != nil {
		return nil, err
	}

	topic := p.server.GetTopic(topicName)
	topic.PutMessages(messages)

	connect.PublishedMessage(topicName, int64(len(messages)))

	return okBytes, nil
}

func (p *Protocol) sub(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	if atomic.LoadInt32(&connect.State) != qnet.StateInit {
		return nil, qerror.MakeError(qerror.INVALID, "SUB connect state invalid")
	}

	if len(params) < 3 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "SUB insufficient number of parameters")
	}

	topicName := string(params[0])
	if !qcore.IsValidName(topicName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "SUB topic name is not valid")
	}

	chanName := string(params[1])
	if !qcore.IsValidName(chanName) {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "SUB channel name is not valid")
	}

	channel := p.server.GetChannel(topicName, chanName)
	err := channel.AddClient(connect.Id, connect)
	if err != nil {
		return nil, err
	}

	atomic.StoreInt32(&connect.State, qnet.StateSubscribed)
	connect.UpdateReadyState(1)

	connect.Channel = channel

	return okBytes, nil
}

func (p *Protocol) req(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	state := atomic.LoadInt32(&connect.State)
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

	maxReqTimeout := qconfig.Q_Config.MaxReqTimeout

	if timeDu < 0 {
		timeDu = 0
	} else if timeDu > maxReqTimeout {
		timeDu = maxReqTimeout
	}

	channel := connect.Channel.(*qcore.Channel)
	err = channel.RequeueMessage(connect.Id, *id, timeDu)
	if err != nil {
		return nil, err
	}

	connect.RequeuedMessage()

	return okBytes, nil
}

func (p *Protocol) fin(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	state := atomic.LoadInt32(&connect.State)
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

	channel := connect.Channel.(*qcore.Channel)
	err = channel.FinishMessage(connect.Id, *id)
	if err != nil {
		return nil, err
	}

	connect.FinishedMessage()

	return nil, nil
}

func (p *Protocol) rdy(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	state := atomic.LoadInt32(&connect.State)

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

	connect.SetReadyCount(count)
	connect.UpdateReadyState(1)

	return nil, nil
}

func (p *Protocol) nop(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	return nil, nil
}

func (p *Protocol) touch(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	state := atomic.LoadInt32(&connect.State)
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

	channel := connect.Channel.(*qcore.Channel)
	err = channel.TouchMessage(connect.Id, *id, connect.MsgTimeout)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (p *Protocol) auth(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	state := atomic.LoadInt32(&connect.State)
	if state != qnet.StateInit {
		return nil, qerror.MakeError(qerror.INVALID, "AUTH connect state invalid")
	}

	if len(params) != 1 {
		return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "AUTH insufficient number of parameters")
	}

	bodyLen, err := readLen(connect.Reader)
	if err != nil {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "AUTH failed to read message body size")
	}

	if bodyLen <= 0 || bodyLen > qconfig.Q_Config.MaxMessageSize {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "AUTH message size is not valid")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(connect.Reader, body)
	if err != nil {
		return nil, err
	}

	if connect.HasAuth() {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "AUTH already set")
	}

	if !connect.IsAuthEnabled() {
		return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "AUTH disabled")
	}

	if err := connect.Auth(string(body)); err != nil {
		return nil, qerror.MakeError(qerror.AUTH_FAILED, "AUTH failed")
	}

	if !connect.HasAuth() {
		return nil, qerror.MakeError(qerror.AUTH_UNAUTH, "AUTH no auth found")
	}

	return okBytes, nil
}

func (p *Protocol) cls(params [][]byte, connect *qnet.TcpConnect) ([]byte, error) {
	state := atomic.LoadInt32(&connect.State)
	if state != qnet.StateSubscribed {
		return nil, qerror.MakeError(qerror.INVALID, "CLS connect state invalid")
	}

	connect.Close()

	return []byte("CLOSE_WAIT"), nil
}
