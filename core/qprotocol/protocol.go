package qprotocol

import (
    "github.com/lnmq/core/qapp"
    "github.com/lnmq/core/qcore"
    "github.com/lnmq/core/qutils"
    "io"
)
import (
    "github.com/lnmq/core/qerror"
    "regexp"
    "encoding/binary"
    "github.com/lnmq/core/qconfig"
)

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

func HandleData(params [][]byte, reader io.Reader) {
    switch params[0] {
    case []byte("FIN"):
    case []byte("RDY"):
    case []byte("REQ"):
    case []byte("PUB"): pub(params, reader)
    case []byte("MPUB"):
    case []byte("DPUB"):
    case []byte("NOP"):
    case []byte("TOUCH"):
    case []byte("SUB"):
    case []byte("CLS"):
    case []byte("AUTH"):
    }
}

func pub(params [][]byte, reader io.Reader) ([]byte, error) {
    var err error

    if len(params) < 2 {
        return nil, qerror.MakeError(qerror.INVALID_PARAMETER, "PUB insufficient number of parameters")
    }

    topicName := string(params[1])
    if !isValidName(topicName) {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB topic name is not valid")
    }

    bodyLen, err := readLen(reader)
    if err != nil {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB failed to read message body size")
    }

    if bodyLen <= 0 || bodyLen > qconfig.GlobalConfig.MaxMessageSize {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB message size is not valid")
    }

    msgBody := make([]byte, bodyLen)
    _, err = io.ReadFull(reader, msgBody)
    if err != nil {
        return nil, qerror.MakeError(qerror.INVALID_MESSAGE, "PUB failed to read message")
    }

    topic := qapp.Q_Server.GetTopic(topicName)
    msg := qcore.NewMessage(qcore.NewMessageId(),msgBody)
    topic.PutMessage(msg)
}