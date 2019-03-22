package qcore

import (
    "sync"
    "bytes"
)

var bq sync.Pool

func init()  {
    bq.New = func() interface{} {
        return &bytes.Buffer{}
    }
}

func GetBufferFromPool() *bytes.Buffer {
    return bq.Get().(*bytes.Buffer)
}

func PutBufferToPool(b *bytes.Buffer)  {
    bq.Put(b)
}