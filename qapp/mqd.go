package main

import (
	"github.com/lnmq/qconfig"
	"github.com/lnmq/qmqd"
)

func main() {
	initConfig()

	qmqd.Q_Server = qmqd.NewServer()
	qmqd.Q_Server.Start()
}

func initConfig() {
	qconfig.Q_Config = qconfig.NewConfig()
}
