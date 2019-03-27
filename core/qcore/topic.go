package qcore

import (
    "fmt"
    "github.com/lnmq/core/qbackend"
    "github.com/lnmq/core/qconfig"
    "sync"
    "sync/atomic"
)

type TopicDeleteCallback interface {
	DeleteTopicCallback(topic *Topic)
}

type Topic struct {
	messageCount uint64
	messageSize  uint64

	mutex sync.Mutex

	Name          string
	channelMap    map[string]*Channel
	backend       BackendQueue
	memoryMsgChan chan *Message

	isNeedUpdateChannel int32
	waitGroup           sync.WaitGroup

	exitFlag int32

	TopicDeleteCallback
	deleter sync.Once

	exitChan chan int32
}

func NewTopic(name string, cb TopicDeleteCallback) *Topic {
	t := &Topic{
		Name:                name,
		channelMap:          make(map[string]*Channel),
		memoryMsgChan:       make(chan *Message, qconfig.Q_Config.MemQueueSize),
		TopicDeleteCallback: cb,
		backend:             &qbackend.EmptyBackendQueue{},
	}

	//backend

	return t
}

func (t *Topic) Start() {

}

func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

func (t *Topic) GetOrCreateChannel(name string) *Channel {
	t.mutex.Lock()

	c, ok := t.channelMap[name]
	if ok {
		t.mutex.Unlock()
		return c
	}

	c = NewChannel(t.Name, name, t)
	t.channelMap[name] = c

	t.mutex.Unlock()

	atomic.StoreInt32(&t.isNeedUpdateChannel, 1)

	return c
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.mutex.Lock()
	channel, ok := t.channelMap[channelName]
	t.mutex.Unlock()

	if !ok {
		return nil, fmt.Errorf("no channel")
	}

	return channel, nil
}

func (t *Topic) DeleteChannel(name string) {
	t.mutex.Lock()
	c, ok := t.channelMap[name]
	if !ok {
		t.mutex.Unlock()
		return
	}

	delete(t.channelMap, name)
	t.mutex.Unlock()

	atomic.StoreInt32(&t.isNeedUpdateChannel, 1)

	c.Delete()
}

func (t *Topic) PutMessage(m *Message) {
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return
	}

	select {
	case t.memoryMsgChan <- m:
	default:
		b := GetBufferFromPool()
		err := writeMessageToBackend(b, m, t.backend)
		PutBufferToPool(b)
		if err != nil {

		}
	}

	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageSize, uint64(len(m.Body)))
}

func (t *Topic) PutMessages(msgs []*Message) {
	for _, v := range msgs {
		t.PutMessage(v)
	}
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

func (t *Topic) messagePump() {
	t.waitGroup.Add(1)
	defer t.waitGroup.Done()

	var msg *Message
	var chans []*Channel

	initChannel := func() {
		t.mutex.Lock()
		chans = make([]*Channel, len(t.channelMap))
		channelNum := 0
		for _, v := range t.channelMap {
			chans[channelNum] = v
			channelNum++
		}
		t.mutex.Unlock()
	}

	initChannel()

	isBreak := false
	for {
		if isBreak {
			break
		}

		select {
		case msg = <-t.memoryMsgChan:
		case <-t.exitChan:
			isBreak = true
			return
		}

		if atomic.CompareAndSwapInt32(&t.isNeedUpdateChannel, 1, 0) {
			initChannel()
		}

		for _, v := range chans {
			cMsg := NewChannelMsg(msg)
            if cMsg.deferred > 0 {
                v.PutDeferredMessage(cMsg)
            } else {
                err := v.PutMessage(cMsg)
                if err != nil {

                }
            }

		}
	}
}

func (t *Topic) Delete() {
	t.exit(true)
}

func (t *Topic) Close() {
	t.exit(false)
}

func (t *Topic) exit(deleted bool) {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return
	}

	if deleted {
		//notify
	}

	close(t.exitChan)

	t.waitGroup.Wait()

	if deleted {
		t.mutex.Lock()

		for _, c := range t.channelMap {
			c.Delete()
		}
		t.channelMap = make(map[string]*Channel)
		t.mutex.Unlock()

		t.Empty()
		t.backend.Delete()

	} else {
		t.mutex.Lock()

		for _, c := range t.channelMap {
			c.Close()
		}

		t.mutex.Unlock()

		t.flush()
		t.backend.Close()
	}
}

func (t *Topic) Empty() {
	isBreak := false
	for {
		if isBreak {
			break
		}

		select {
		case <-t.memoryMsgChan:
		default:
			isBreak = true
		}
	}

	t.backend.Empty()
}

func (t *Topic) flush() {
	buf := GetBufferFromPool()

	isBreak := false
	for {
		if isBreak {
			break
		}
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(buf, msg, t.backend)
			if err != nil {

			}
		default:
			isBreak = true
			break
		}
	}
}

func (t *Topic) DeleteChannelCallback(channel *Channel) {
	t.DeleteChannel(channel.Name)
}
