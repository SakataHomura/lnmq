package qmqd

import (
    "github.com/lnmq/qconfig"
    "time"
)

type Config struct {
    Base qconfig.Config

	MaxReqTimeout       time.Duration
	MaxMessageSize      int32
	MaxBodySize         int32

	AuthHttpAddresses        []string
	HttpClientConnectTimeout time.Duration
	HttpClientRequestTimeout time.Duration

    QueueScanSelectionCount int32
    QueueScanInterval time.Duration
    QueueRefreshInterval time.Duration
    QueueScanDirtyPercent int32
}

func NewConfig() *Config {
	c := &Config{
        Base:qconfig.Config{
            OutputBufferTimeout: 250 * time.Millisecond,
            MsgTimeout:          60 * time.Second,
            ClientTimeout:       60 * time.Second,
            TcpAddress:          "127.0.0.1:8100",
            MemQueueSize:        10000,
        },
		MaxMessageSize:      1024 * 1024,
		MaxBodySize:         5 * 1024 * 1024,
	}

	qconfig.Q_Config = &c.Base

	return c
}

var Q_Config *Config
