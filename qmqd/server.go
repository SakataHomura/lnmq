package qmqd

import (
    "github.com/lnmq/qcore"
    "github.com/lnmq/qnet"
    "github.com/lnmq/qutils"
    "sync"
    "time"
)

var Q_Server *Server

type Server struct {
	clientId int64

	topicMap  map[string]*qcore.Topic
	tcpServer *qnet.TcpServer

	rwMutex sync.RWMutex

	updateChannelChan chan *qcore.Channel
}

func NewServer() *Server {
	s := &Server{
		topicMap: make(map[string]*qcore.Topic),
	}

	s.tcpServer = qnet.NewTcpServer(createTcpClient, Q_Config.Base)

	return s
}

func (s *Server) Start() {
	s.tcpServer.Start(Q_Config.Base)
}

func (s *Server) DeleteTopic(name string) {
	s.rwMutex.Lock()
	t, ok := s.topicMap[name]
	if !ok {
		s.rwMutex.Unlock()
		return
	}

	delete(s.topicMap, name)
	s.rwMutex.Unlock()

	t.Delete()
}

func (s *Server) DeleteTopicCallback(topic *qcore.Topic) {
	s.rwMutex.Lock()

	t, ok := s.topicMap[topic.Name]
	if !ok {
		s.rwMutex.Unlock()
		return
	}

	delete(s.topicMap, topic.Name)

	s.rwMutex.Unlock()

	t.Delete()
}

func (s *Server) GetTopic(name string) *qcore.Topic {
	s.rwMutex.RLock()
	t, ok := s.topicMap[name]
	s.rwMutex.RUnlock()
	if ok {
		return t
	}

	s.rwMutex.Lock()
	t, ok = s.topicMap[name]
	if ok {
		s.rwMutex.Unlock()
		return t
	}

	t = qcore.NewTopic(name, s)
	s.topicMap[name] = t
	s.rwMutex.Unlock()

	t.Start()

	return t
}

func (s *Server) GetChannel(topicName, chanName string) *qcore.Channel {
	topic := s.GetTopic(topicName)

	c, isNew := topic.GetOrCreateChannel(chanName)
	if isNew {
		select {
		case s.updateChannelChan <- c:
		}
	}

	return c
}

func (s *Server) GetAllChannel() []*qcore.Channel {
    ret := make([]*qcore.Channel, 0)

    s.rwMutex.RLock()

    for _, t := range s.topicMap {
        c := t.GetAllChannels()
        ret = append(ret, c...)
    }

    s.rwMutex.RUnlock()

    return ret
}

func (s *Server) queueLoop()  {
    workCh := make(chan *qcore.Channel, Q_Config.QueueScanSelectionCount)
    retCh := make(chan bool, Q_Config.QueueScanSelectionCount)

    workTicker := time.NewTicker(Q_Config.QueueScanInterval)
    refreshTicker := time.NewTicker(Q_Config.QueueRefreshInterval)

    channels := s.GetAllChannel()

    isBreak := false
    for {
        if isBreak {
            break
        }

        select {
        case <-workTicker.C:
            if len(channels) == 0 {
                continue
            }
        case <-refreshTicker.C:
            newChans := make([]*qcore.Channel, 0, len(channels))
            for _, ch := range channels {
                if ch.Exiting() {
                    continue
                }

                newChans = append(newChans, ch)
            }
            channels = newChans
            continue
        case c := <-s.updateChannelChan:
            if !c.Exiting() {
                channels = append(channels, c)
            }
            continue
        }

        num := int(Q_Config.QueueScanSelectionCount)
        if num > len(channels) {
            num = len(channels)
        }

        for  {
            workNum := 0
            for _, i := range qutils.UniqRands(num, len(channels))  {
                if channels[i].Exiting() {
                    continue
                }

                workCh <- channels[i]
                workNum ++
            }

            dirty := 0
            for i:=0; i<workNum; i++ {
                if <- retCh {
                    dirty ++
                }
            }

            if int32(dirty * 100 / workNum) <= Q_Config.QueueScanDirtyPercent {
                break
            }
        }
    }

    workTicker.Stop()
    refreshTicker.Stop()
}