package qcore

import (
    "encoding/hex"
    "github.com/lnmq/core/qutils"
    "time"
    "io"
    "encoding/binary"
    "fmt"
    "bytes"
)

const (
    MsgIdLength = 16
    minValidMsgLength = MsgIdLength + 8 + 2
)

type MessageId [MsgIdLength]byte

type Message struct {
    Id MessageId
    Body []byte
    Timestamp int64
    Attempts uint16
}

func NewMessageId() MessageId {
    id := qutils.IdGenerater.NewGuid()

    h := MessageId{}
    b := [8]byte{}

    b[0] = byte(id >> 56)
    b[1] = byte(id >> 48)
    b[2] = byte(id >> 40)
    b[3] = byte(id >> 32)
    b[4] = byte(id >> 24)
    b[5] = byte(id >> 16)
    b[6] = byte(id >> 8)
    b[7] = byte(id)

    hex.Encode(h[:], b[:])

    return h
}

func NewMessage(id MessageId, body []byte) *Message {
    return &Message{
        Id:id,
        Body:body,
        Timestamp:time.Now().UnixNano(),
    }
}

func (m *Message) WriteTo(w io.Writer) (int, error) {
    buf := [10]byte{}
    total := 0

    binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
    binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

    n, err := w.Write(buf[:])
    total += n
    if err != nil {
        return total, err
    }

    n, err = w.Write(m.Id[:])
    total += n
    if err != nil {
        return total, err
    }

    n, err = w.Write(m.Body)
    total += n
    if err != nil {
        return total, err
    }

    return total, nil
}

func decodeMessage(b []byte) (*Message, error) {
    msg := Message{}

    if len(b) < minValidMsgLength {
        return nil, fmt.Errorf("invalid message buffer size")
    }

    msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
    msg.Attempts = binary.BigEndian.Uint16(b[8:10])
    copy(msg.Id[:], b[10:10+MsgIdLength])
    msg.Body = b[10+MsgIdLength:]

    return &msg, nil
}

func writeMessageToBackend(buf *bytes.Buffer, msg *Message, q BackendQueue) error {
    buf.Reset()

    _, err := msg.WriteTo(buf)
    if err != nil {
        return err
    }

    return q.Put(buf.Bytes())
}