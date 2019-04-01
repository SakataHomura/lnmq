package qcore

import (
	"container/heap"
	"fmt"
	"github.com/lnmq/qbackend"
	"github.com/lnmq/qconfig"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type ChannelDeleteCallback interface {
	DeleteChannelCallback(channel *Channel)
}

type Channel struct {
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	topicName string
	Name      string
	exitFlag  int32

	backend BackendQueue

	MemoryMsgChan chan *ChannelMsg

	mutex     sync.Mutex
	consumers map[uint64]Consumer

	ChannelDeleteCallback
	deleter sync.Once

	deferredMutex    sync.Mutex
	deferredMessages map[MessageId]*PriorityObject
	deferredQ        PriorityQueue

	inFlightMutex    sync.Mutex
	inFlightMessages map[MessageId]*PriorityObject
	inFlightQ        PriorityQueue
}

type ChannelMsg struct {
	*Message

	deliveryTs time.Time
	clientId   uint64
}

func NewChannelMsg(msg *Message) *ChannelMsg {
	c := &ChannelMsg{}

	c.Message = msg
	c.Timestamp = msg.Timestamp

	return c
}

func NewChannel(topicName string, name string, callback ChannelDeleteCallback) *Channel {
	c := &Channel{
		topicName:             topicName,
		Name:                  name,
		MemoryMsgChan:         make(chan *ChannelMsg, qconfig.Q_Config.MemQueueSize),
		consumers:             make(map[uint64]Consumer),
		ChannelDeleteCallback: callback,
		backend:               &qbackend.EmptyBackendQueue{},
	}

	c.initQ()

	//notify

	return c
}

func (c *Channel) initQ() {
	qSize := int(math.Max(1, float64(qconfig.Q_Config.MemQueueSize/10)))

	c.inFlightMutex.Lock()
	c.inFlightMessages = make(map[MessageId]*PriorityObject)
	c.inFlightQ = NewPriorityQueue(qSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredMessages = make(map[MessageId]*PriorityObject)
	c.deferredQ = NewPriorityQueue(qSize)
	c.deferredMutex.Unlock()
}

func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

func (c *Channel) Delete() {
	c.exit(true)
}

func (c *Channel) Close() {
	c.exit(false)
}

func (c *Channel) exit(deleted bool) {
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return
	}

	if deleted {
		//notify
	}

	c.mutex.Lock()
	for _, v := range c.consumers {
		v.Close()
	}
	c.mutex.Unlock()

	if deleted {
		c.Empty()
		c.backend.Delete()
	} else {
		c.flush()
		c.backend.Close()
	}
}

func (c *Channel) Empty() {
	c.mutex.Lock()

	c.initQ()
	for _, v := range c.consumers {
		v.Empty()
	}

	isBreak := false
	for {
		if isBreak {
			break
		}

		select {
		case <-c.MemoryMsgChan:
		default:
			isBreak = true
		}
	}

	c.mutex.Unlock()

	c.backend.Empty()
}

func (c *Channel) flush() {
	buf := GetBufferFromPool()

	isBreak := false
	for {
		if isBreak {
			break
		}

		select {
		case msg := <-c.MemoryMsgChan:
			writeMessageToBackend(buf, msg.Message, c.backend)
		default:
			isBreak = true
		}
	}

	c.inFlightMutex.Lock()
	for _, o := range c.inFlightMessages {
		msg := o.Value.(*ChannelMsg)
		writeMessageToBackend(buf, msg.Message, c.backend)
	}
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	for _, o := range c.deferredMessages {
		msg := o.Value.(*ChannelMsg)
		writeMessageToBackend(buf, msg.Message, c.backend)
	}
	c.deferredMutex.Unlock()

	PutBufferToPool(buf)
}
func (c *Channel) Depth() int64 {
	return int64(len(c.MemoryMsgChan)) + c.backend.Depth()
}

func (c *Channel) PutMessage(m *ChannelMsg) error {
	if c.Exiting() {
		return fmt.Errorf("exiting")
	}

	select {
	case c.MemoryMsgChan <- m:
	default:
		buf := GetBufferFromPool()
		writeMessageToBackend(buf, m.Message, c.backend)
		PutBufferToPool(buf)
	}

	atomic.AddUint64(&c.messageCount, 1)

	return nil
}

func (c *Channel) PutDeferredMessage(m *ChannelMsg) {
	c.StartDeferredTimeout(m)

	atomic.AddUint64(&c.messageCount, 1)
}

func (c *Channel) TouchMessage(clientId uint64, id MessageId, timeout time.Duration) error {
	newTimeout := time.Now().Add(timeout)

	c.inFlightMutex.Lock()

	o, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return fmt.Errorf("no data")
	}

	msg := o.Value.(*ChannelMsg)
	if msg.clientId != clientId {
		c.inFlightMutex.Unlock()
		return fmt.Errorf("no data")
	}

	heap.Remove(&c.inFlightQ, o.Index)

	o.Priority = newTimeout.UnixNano()

	heap.Push(&c.inFlightQ, o.Index)

	c.inFlightMutex.Unlock()

	return nil
}

func (c *Channel) FinishMessage(clientId uint64, id MessageId) error {
	msg, err := c.popInFlightMessage(clientId, id)
	if err != nil {
		return err
	}

	c.removeFromFlightQ(msg)

	return nil
}

func (c *Channel) RequeueMessage(clientId uint64, id MessageId, timeout time.Duration) error {
	o, err := c.popInFlightMessage(clientId, id)
	if err != nil {
		return err
	}

	atomic.AddUint64(&c.requeueCount, 1)

	if timeout == 0 {
		if c.Exiting() {
			return fmt.Errorf("exiting")
		}

		err = c.PutMessage(o.Value.(*ChannelMsg))
		return err
	}

	msg := o.Value.(*ChannelMsg)
	msg.Deferred = timeout

	return c.StartDeferredTimeout(msg)
}

func (c *Channel) AddClient(clientId uint64, consumer Consumer) error {
	c.mutex.Lock()

	//fusion
	c.consumers[clientId] = consumer

	c.mutex.Unlock()

	return nil
}

func (c *Channel) RemoveClient(clientId uint64) {
	c.mutex.Lock()

	delete(c.consumers, clientId)

	c.mutex.Unlock()
}

func (c *Channel) StartInFlightTimeout(msg *ChannelMsg, clientId int64, timeout time.Duration) error {
	now := time.Now()
	msg.deliveryTs = now
	o := &PriorityObject{
		Value:    msg,
		Priority: now.Add(timeout).UnixNano(),
	}

	c.inFlightMutex.Lock()

	_, ok := c.inFlightMessages[msg.Id]
	if ok {
		c.inFlightMutex.Unlock()
		return fmt.Errorf("msg exist")
	}

	c.inFlightMessages[msg.Id] = o
	heap.Push(&c.inFlightQ, o)

	c.inFlightMutex.Unlock()

	return nil
}

func (c *Channel) StartDeferredTimeout(m *ChannelMsg) error {
	ts := time.Now().Add(m.Deferred).UnixNano()
	o := &PriorityObject{
		Value:    m,
		Priority: ts,
	}

	c.deferredMutex.Lock()

	id := o.Value.(*ChannelMsg).Id
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return nil
	}

	c.deferredMessages[id] = o
	heap.Push(&c.deferredQ, o)

	c.deferredMutex.Unlock()

	return nil
}

func (c *Channel) popInFlightMessage(clientId uint64, id MessageId) (*PriorityObject, error) {
	c.inFlightMutex.Lock()

	o, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, fmt.Errorf("msg exist")
	}

	msg := o.Value.(*ChannelMsg)
	if msg.clientId != clientId {
		c.inFlightMutex.Unlock()
		return nil, fmt.Errorf("msg exist")
	}

	delete(c.inFlightMessages, id)

	if o.Index != -1 {
		heap.Remove(&c.inFlightQ, o.Index)
	}

	c.inFlightMutex.Unlock()

	return o, nil
}

func (c *Channel) removeFromFlightQ(msg *PriorityObject) {
	if msg.Index == -1 {
		return
	}

	c.inFlightMutex.Lock()

	heap.Remove(&c.inFlightQ, msg.Index)

	c.inFlightMutex.Unlock()
}

func (c *Channel) popDeferredMessage(clientId uint64, id MessageId) (*PriorityObject, error) {
	c.deferredMutex.Lock()

	o, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, fmt.Errorf("msg not exist")
	}

	msg := o.Value.(*ChannelMsg)
	if msg.clientId != clientId {
		c.deferredMutex.Unlock()
		return nil, fmt.Errorf("msg not exist")
	}

	delete(c.deferredMessages, id)

	if o.Index != -1 {
		heap.Remove(&c.deferredQ, o.Index)
	}

	c.deferredMutex.Unlock()

	return o, nil
}

func (c *Channel) processDeferredQueue(t int64) bool {
	if c.Exiting() {
		return false
	}

	dirty := false
	c.deferredMutex.Lock()
	for {
		o, _ := c.deferredQ.PeekAndShift(t)
		if o == nil {
			break
		}

		dirty = true
		msg := o.Value.(*ChannelMsg)
		delete(c.deferredMessages, msg.Id)

		c.PutMessage(msg)
	}

	c.deferredMutex.Unlock()

	return dirty
}

func (c *Channel) processInFlightQueue(t int64) bool {
	if c.Exiting() {
		return false
	}

	dirty := false
	c.inFlightMutex.Lock()
	for {
		o, _ := c.inFlightQ.PeekAndShift(t)
		if o == nil {
			break
		}

		dirty = true

		msg := o.Value.(*ChannelMsg)
		delete(c.inFlightMessages, msg.Id)

		atomic.AddUint64(&c.timeoutCount, 1)

		c.mutex.Lock()
		consume, ok := c.consumers[msg.clientId]
		c.mutex.Unlock()
		if ok {
			consume.TimedOutMessage()
		}

		c.PutMessage(msg)

	}
	c.inFlightMutex.Unlock()

	return dirty
}
