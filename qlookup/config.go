package qlookup

import "github.com/lnmq/qconfig"

type Config struct {
    Base qconfig.Config

	MaxMessageSize int32
}

func NewConfig() *Config {
	c := &Config{
	    Base:qconfig.Config{

        },
    }

	qconfig.Q_Config = &c.Base

	return c
}

var Q_Config *Config
