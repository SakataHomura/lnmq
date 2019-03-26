package main

import (
    "github.com/lnmq/core/qapp"
    "github.com/lnmq/core/qconfig"
)

func main() {
	initConfig()

	qapp.Q_Server = qapp.NewServer()
	qapp.Q_Server.Start()
}

func initConfig() {
    qconfig.Q_Config = qconfig.NewConfig()
}
