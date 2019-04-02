package qconfig

import "time"

type Config struct {
    OutputBufferTimeout time.Duration
    MsgTimeout          time.Duration
    ClientTimeout       time.Duration

    TcpAddress string
    HttpAddress      string
    BroadcastAddress string

    MemQueueSize        int32
}

var Q_Config *Config