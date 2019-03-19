package config

import "time"

type Config struct {
    OutputBufferTimeout time.Duration
    MsgTimeout    time.Duration
    ClientTimeout time.Duration

}

var GlobalConfig Config
