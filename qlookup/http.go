package qlookup

import (
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/lnmq/qcore"
	"github.com/lnmq/qhttp"
	"github.com/lnmq/qversion"
	"net/http"
)

type httpServer struct {
	router http.Handler
	server *LookupServer
}

func NewHttpServer() *httpServer {
	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = qhttp.HttpPanicHandler()
	router.NotFound = qhttp.HttpNotFoundHandler()
	router.MethodNotAllowed = qhttp.MethodNotAllowedHandler()

	s := &httpServer{
		router: router,
	}

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) ping(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *httpServer) info(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: qversion.Version,
	}, nil
}

func (s *httpServer) topics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topics := s.server.FindLookupKey("topic", "*", "")
	keys := LookupKeys(topics)

	return map[string]interface{}{
		"topics": keys,
	}, nil
}

func (s *httpServer) channels(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := qhttp.NewReqParams(req)
	if err != nil {
		return nil, qhttp.HttpErr{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, qhttp.HttpErr{400, "MISSING_ARG_TOPIC"}
	}

	channels := s.server.FindLookupKey("channel", topicName, "*")
	subkeys := LookupSubkeys(channels)

	return map[string]interface{}{
		"channels": subkeys,
	}, nil
}

func (s *httpServer) lookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := qhttp.NewReqParams(req)
	if err != nil {
		return nil, qhttp.HttpErr{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, qhttp.HttpErr{400, "MISSING_ARG_TOPIC"}
	}

	lookups := s.server.FindLookupKey("topic", topicName, "")
	if len(lookups) == 0 {
		return nil, qhttp.HttpErr{400, "TOPIC_NOT_FOUND"}
	}

	channels := s.server.FindLookupKey("topic", topicName, "*")
	ckeys := LookupKeys(channels)
	producers := s.server.FindProduces("topic", topicName, "")
	infos := ProducerPeerInfo(producers)

	return map[string]interface{}{
		"channels":  ckeys,
		"producers": infos,
	}, nil
}

func (s *httpServer) createTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := qhttp.NewReqParams(req)
	if err != nil {
		return nil, qhttp.HttpErr{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, qhttp.HttpErr{400, "MISSING_ARG_TOPIC"}
	}

	if !qcore.IsValidName(topicName) {
		return nil, qhttp.HttpErr{400, "INVALID_ARG_TOPIC"}
	}

	k := LookupKey{"topic", topicName, ""}
	s.server.AddLookup(k)

	return nil, nil
}

func (s *httpServer) deleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := qhttp.NewReqParams(req)
	if err != nil {
		return nil, qhttp.HttpErr{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, qhttp.HttpErr{400, "MISSING_ARG_TOPIC"}
	}

	lookups := s.server.FindLookupKey("channel", topicName, "*")
	for _, v := range lookups {
		s.server.RemoveLookup(v)
	}

	lookups = s.server.FindLookupKey("topic", topicName, "")
	for _, v := range lookups {
		s.server.RemoveLookup(v)
	}

	return nil, nil
}

func (s *httpServer) createChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := qhttp.NewReqParams(req)
	if err != nil {
		return nil, qhttp.HttpErr{400, "INVALID_REQUEST"}
	}

	topic, channel, err := GetTopicChannel(reqParams)
	if err != nil {
		return nil, qhttp.HttpErr{400, err.Error()}
	}

	key := LookupKey{"channel", topic, channel}
	s.server.AddLookup(key)

	key = LookupKey{"topic", topic, ""}
	s.server.AddLookup(key)

	return nil, nil
}

func (s *httpServer) deleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := qhttp.NewReqParams(req)
	if err != nil {
		return nil, qhttp.HttpErr{400, "INVALID_REQUEST"}
	}

	topic, channel, err := GetTopicChannel(reqParams)
	if err != nil {
		return nil, qhttp.HttpErr{400, err.Error()}
	}

	keys := s.server.FindLookupKey("channel", topic, channel)
	if len(keys) == 0 {
		return nil, qhttp.HttpErr{404, "CHANNEL_NOT_FOUND"}
	}

	for _, k := range keys {
		s.server.RemoveLookup(k)
	}

	return nil, nil
}

type node struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TcpPort          int32    `json:"tcp_port"`
	HttpPort         int32    `json:"http_port"`
	Version          string   `json:"version"`
	Topics           []string `json:"topics"`
}

func (s *httpServer) nodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	producers := s.server.FindProduces("client", "", "")
	nodes := make([]*node, len(producers))

	for i, p := range producers {
		topics := s.server.keys(p.peerInfo.id)
		keys := LookupKeys(topics)

		nodes[i] = &node{
			RemoteAddress:    p.peerInfo.RemoreAddress,
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TcpPort:          p.peerInfo.TcpPort,
			HttpPort:         p.peerInfo.HttpPort,
			Version:          p.peerInfo.Version,
			Topics:           keys,
		}
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

func GetTopicChannel(req *qhttp.ReqParams) (string, string, error) {
	topicName, err := req.Get("topic")
	if err != nil {
		return "", "", fmt.Errorf("MISSING_ARG_TOPIC")
	}

	if qcore.IsValidName(topicName) {
		return "", "", fmt.Errorf("INVALID_ARG_TOPIC")
	}

	channelName, err := req.Get("channel")
	if err != nil {
		return "", "", fmt.Errorf("MISSING_ARG_CHANNEL")
	}

	if qcore.IsValidName(channelName) {
		return "", "", fmt.Errorf("INVALID_ARG_CHANNEL")
	}

	return topicName, channelName, nil
}
