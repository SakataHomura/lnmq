package qconfig

import "time"

type Config struct {
    OutputBufferTimeout time.Duration
    MsgTimeout    time.Duration
    ClientTimeout time.Duration
    MaxMessageSize int32
    MemQueueSize int32

    TCPAddress string
}

var GlobalConfig Config
