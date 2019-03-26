package main

import (
	"fmt"
	"micro-loan/common/tools"

	"github.com/bitly/go-nsq"
)

func sendMsg(str string) {
	config := nsq.NewConfig()
	w, _ := nsq.NewProducer("127.0.0.1:4150", config)
	err := w.Ping()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = w.Publish("test", []byte(str))
	if err != nil {
		fmt.Println(err)
		return
	}

	w.Stop()
}

func main() {
	sendMsg("mytest:" + tools.Int2Str(1))
}
