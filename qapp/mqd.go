package main

import (
    "github.com/lnmq/qmqd"
)

func main() {
	initConfig()

	qmqd.Q_Server = qmqd.NewServer()
	qmqd.Q_Server.Start()
}

func initConfig() {
	qmqd.Q_Config = qmqd.NewConfig()
}
