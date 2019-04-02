package qmqd

import "time"

type Config struct {
	OutputBufferTimeout time.Duration
	MsgTimeout          time.Duration
	ClientTimeout       time.Duration
	MaxReqTimeout       time.Duration
	MaxMessageSize      int32
	MaxBodySize         int32
	MemQueueSize        int32

	TCPAddress string

	AuthHttpAddresses        []string
	HttpClientConnectTimeout time.Duration
	HttpClientRequestTimeout time.Duration
}

func NewConfig() *Config {
	return &Config{
		OutputBufferTimeout: 250 * time.Millisecond,
		MsgTimeout:          60 * time.Second,
		ClientTimeout:       60 * time.Second,
		MaxMessageSize:      1024 * 1024,
		MaxBodySize:         5 * 1024 * 1024,
		MemQueueSize:        10000,
		TCPAddress:          "127.0.0.1:8100",
	}
}

var Q_Config *Config
