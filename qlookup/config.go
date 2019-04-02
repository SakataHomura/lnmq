package qlookup

type Config struct {
	MaxMessageSize int32

	TcpAddress       string
	HttpAddress      string
	BroadcastAddress string
}

func NewConfig() *Config {
	return &Config{}
}

var Q_Config *Config
