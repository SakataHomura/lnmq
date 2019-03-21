package qcore

import (
    "sync"
    "github.com/lnmq/core/qconfig"
    "sync/atomic"
)

type TopicDeleteCallback interface {
    DeleteTopicCallback(topic *Topic)
}

type Topic struct {
    messageCount uint64
    messageSize  uint64

    lock sync.Mutex

    name string
    channelMap map[string]*Channel
    //backend
    memoryMsgChan chan *Message

    deleteCallback func(*Topic)
    deleter sync.Once

    exitChan chan int32
}

func NewTopic(name string, cb TopicDeleteCallback) *Topic {
    t := &Topic{
        name:name,
        channelMap:make(map[string]*Channel),
        memoryMsgChan:make(chan *Message, qconfig.GlobalConfig.MemQueueSize),
    }

    //backend

    return t
}

func (t *Topic) Start() {
    var msg *Message
    chans := make([]*Channel, len(t.channelMap))
    channelNum := 0
    for _, v:=range t.channelMap {
        chans[channelNum] = v
        channelNum ++
    }

    for {
        select {
        case msg = <- t.memoryMsgChan:
        case <- t.exitChan:
            //exit
            return
       }

        for _, v := range chans {
            cMsg := NewChannelMsg(msg)

            err := v.PutMessage(cMsg)
            if err != nil {

            }
        }
    }
}

func (t *Topic) Delete() {

}

func (t *Topic) PutMessage(m *Message) {
    select {
    case t.memoryMsgChan <- m:
    }

    atomic.AddUint64(&t.messageCount, 1)
    atomic.AddUint64(&t.messageSize, uint64(len(m.Body)))
}