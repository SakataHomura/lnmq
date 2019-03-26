package qapp

import (
	"github.com/lnmq/core/qcore"
	"github.com/lnmq/core/qnet"
	"sync"
)

var Q_Server *Server

type Server struct {
	clientId int64

	topicMap map[string]*qcore.Topic
	tcpServer *qnet.TcpServer

	rwMutex sync.RWMutex
}

func NewServer() *Server {
	s := &Server{
		topicMap:make(map[string]*qcore.Topic),
	}

	s.tcpServer.Create()

	return s
}

func (s *Server) Start()  {
    s.tcpServer.Start()
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

func (s *Server) DeleteTopicCallback(topic *qcore.Topic)() {
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

     return topic.GetOrCreateChannel(chanName)
}