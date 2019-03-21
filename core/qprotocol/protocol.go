package qprotocol

import (
    "github.com/lnmq/core/qapp"
    "github.com/lnmq/core/qcore"
    "io"
    "regexp"
    "encoding/binary"
    "github.com/lnmq/core/qerror"
    "github.com/lnmq/core/qconfig"
    "sync/atomic"
    "github.com/lnmq/core/qnet"
    "time"
    "bytes"
)

var okBytes = []byte("OK")
var heartbeatBytes = []byte("_heartbeat_")

type Protocol struct {

}

var validTopicFormatRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
var lenBuf = [4]byte{}

func isValidName(name string) bool {
    if len(name) > 64 || len(name) < 1 {
        return false
    }

    return validTopicFormatRegex.MatchString(name)
}

func readLen(r io.Reader) (int32, error) {
    var tmp []byte = lenBuf[:]
    _, err := io.ReadFull(r, tmp)
    if err != nil {
        return 0, err
    }

    return int32(binary.BigEndian.Uint32(tmp)), nil
}

func (p *Protocol) MessageLoop(conn *qnet.TcpConnect) {
    var messageChan chan *qcore.ChannelMsg
    var channel *qcore.Channel
    heartbeatTicker := time.NewTicker(conn.HeartbeatInterval)
    heartbeatChan := heartbeatTicker.C

    for {
        if channel == nil {
             messageChan = nil
        } else {

        }

        select {
        case state := <- conn.ReadyStateChan:
            if state == qnet.StateSubscribed {
                channel = qapp.Q_Server.GetChannel(conn.TopicName, conn.ChannelName)
                messageChan = channel.MemoryMsgChan
            }
        case <- heartbeatChan:
            _, err :=conn.Send(heartbeatBytes)
            if err != nil {
                //...
            }
        case msg := <- messageChan:
            buf := &bytes.Buffer{}
            _, err :=msg.WriteTo(buf)
            if err == nil {
                _, err =conn.Send(buf.Bytes())
            }
            if err != nil {

            }
        }
    }
}

func (p *Protocol) ConnectDataHandle(params [][]byte, connect qnet.TcpConnect) {
    switch params[0] {
    case []byte("FIN"):
    case []byte("RDY"):
    case []byte("REQ"):
    case []byte("PUB"): pub(params, connect)
    case []byte("MPUB"):
    case []byte("DPUB"):
    case []byte("NOP"):
    case []byte("TOUCH"):
    case []byte("SUB"): sub(params, connect)
    case []byte("CLS"):
    case []byte("AUTH"):
    }
}

func pub(params [][]byte, connect qnet.TcpConnect) ([]byte, error) {
    var err error

    if len(params) < 2 {
        return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "PUB insufficient number of parameters")
    }

    topicName := string(params[1])
    if !isValidName(topicName) {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB topic name is not valid")
    }

    bodyLen, err := readLen(connect.Reader)
    if err != nil {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB failed to read message body size")
    }

    if bodyLen <= 0 || bodyLen > qconfig.GlobalConfig.MaxMessageSize {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB message size is not valid")
    }

    msgBody := make([]byte, bodyLen)
    _, err = io.ReadFull(connect.Reader, msgBody)
    if err != nil {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB failed to read message")
    }

    topic := qapp.Q_Server.GetTopic(topicName)
    msg := qcore.NewMessage(qcore.NewMessageId(),msgBody)
    topic.PutMessage(msg)

    return okBytes, nil
}

func sub(params [][]byte, connect qnet.TcpConnect) ([]byte, error) {
    if atomic.LoadInt32(&connect.State) != qnet.StateInit {
        return nil, qerror.MakeError(qerror.INVALID, "SUB connect state invalid")
    }

    if len(params) < 3 {
        return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "SUB insufficient number of parameters")
    }

    topicName := string(params[0])
    if !isValidName(topicName) {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "SUB topic name is not valid")
    }

    chanName := string(params[1])
    if !isValidName(chanName) {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "SUB channel name is not valid")
    }

    channel := qapp.Q_Server.GetChannel(topicName, chanName)
    err := channel.AddClient(connect.Id, &connect)
    if err != nil {
        return nil, err
    }

    atomic.StoreInt32(&connect.State, qnet.StateSubscribed)

    return okBytes, nil
}