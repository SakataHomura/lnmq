package qcore

import (
    "time"
    "sync"
    "github.com/lnmq/core/qconfig"
    "sync/atomic"
)

type ChannelDeleteCallback interface {
    Do(topic *Topic)
}

type Channel struct {
    requeueCount uint64
    messageCount uint64
    timeoutCount uint64

    topicName string
    name string

    memoryMsgChan chan *ChannelMsg

    consumers map[int64]*Consumer

    deleteCallback ChannelDeleteCallback
    deleter sync.Once
}

type ChannelMsg struct {
    *Message

    deliveryTs time.Time
    clientId int64
    pri int64
    index int32
}

func NewChannel(topicName string, name string, callback ChannelDeleteCallback) *Channel {
    c := &Channel{
        topicName:topicName,
        name:name,
        memoryMsgChan:make(chan *ChannelMsg, qconfig.GlobalConfig.MemQueueSize),
        consumers:make(map[int64]*Consumer),
        deleteCallback:callback,
    }

    return c
}

func NewChannelMsg(msg *Message) *ChannelMsg {
    c := &ChannelMsg{}

    c.Message = msg
    c.Timestamp = msg.Timestamp

    return c
}

func (c *Channel) PutMessage(m *ChannelMsg) error {
    select {
    case c.memoryMsgChan <- m:
    }

    atomic.AddUint64(&c.messageCount, 1)

    return nil
}

