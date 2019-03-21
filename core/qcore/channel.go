package qcore

import (
    "time"
    "sync"
    "github.com/lnmq/core/qconfig"
    "sync/atomic"
)

type ChannelDeleteCallback interface {
    DeleteChannelCallback(channel *Channel)
}

type Channel struct {
    requeueCount uint64
    messageCount uint64
    timeoutCount uint64

    topicName string
    name string

    MemoryMsgChan chan *ChannelMsg

    consumers map[uint64]Consumer

    ChannelDeleteCallback
    deleter sync.Once

    lock sync.Mutex
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
        MemoryMsgChan:make(chan *ChannelMsg, qconfig.GlobalConfig.MemQueueSize),
        consumers:make(map[uint64]Consumer),
        ChannelDeleteCallback:callback,
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
    case c.MemoryMsgChan <- m:
    }

    atomic.AddUint64(&c.messageCount, 1)

    return nil
}

func (c *Channel) AddClient(clientId uint64, consumer Consumer) error {
    c.lock.Lock()

    //fusion
    c.consumers[clientId] = consumer

    c.lock.Unlock()

    return nil
}

func (c *Channel) Delete() {

}