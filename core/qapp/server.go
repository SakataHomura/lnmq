package qapp

import (
	"github.com/lnmq/core/qcore"
	"github.com/lnmq/core/qnet"
	"sync"
)

var Q_Server Server

type Server struct {
	clientId int64

	topicMap map[string]*qcore.Topic
	tcpServer *qnet.TcpServer

	lock sync.RWMutex
}

func NewServer() *Server {
	s := &Server{
		topicMap:make(map[string]*qcore.Topic),
	}

	return s
}

func (s *Server) DeleteTopic(name string) {
	s.lock.Lock()
	t, ok := s.topicMap[name]
	if !ok {
		s.lock.Unlock()
		return
	}

	delete(s.topicMap, name)
	s.lock.Unlock()

	t.Delete()
}

func (s *Server) DeleteTopicCallback(topic *qcore.Topic)() {

}

func (s *Server) GetTopic(name string) *qcore.Topic {
	s.lock.RLock()
	t, ok := s.topicMap[name]
	s.lock.RUnlock()
	if ok {
		return t
	}

	s.lock.Lock()
	t, ok = s.topicMap[name]
	if ok {
		s.lock.Unlock()
		return t
	}

	t = qcore.NewTopic(name, s)
	s.topicMap[name] = t
	s.lock.Unlock()

	t.Start()

	return t
}