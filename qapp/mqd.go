package main

import (
	"github.com/lnmq/qcore/qconfig"
	"github.com/lnmq/qcore/qmqd"
)

func main() {
	initConfig()

    qmqd.Q_Server = qmqd.NewServer()
    qmqd.Q_Server.Start()
}

func initConfig() {
	qconfig.Q_Config = qconfig.NewConfig()
}
