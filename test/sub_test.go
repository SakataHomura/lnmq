package main

import (
    "fmt"

    "github.com/bitly/go-nsq"
)

func main() {
    for i := 1; i <= 3; i++ {
        go start(i)
    }

    var c chan int
    <-c
}

func start(i int) {
    config := nsq.NewConfig()
    c := "ch" //+ tools.Int2Str(i)
    q, _ := nsq.NewConsumer("test", c, config)
    q.AddHandler(nsq.HandlerFunc(func(mes *nsq.Message) error {
        fmt.Println("message:%s:%s", c, string(mes.Body))
        mes.Finish()
        return nil
    }))

    err := q.ConnectToNSQLookupd("127.0.0.1:4161")
    if err != nil {
        fmt.Println(err)
        return
    }

    <-q.StopChan
}
