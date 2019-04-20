package qbackend

import (
	"bufio"
	"bytes"
	"os"
	"sync"
	"time"
)

type DiskQueue struct {
	readPos int64
	writePos int64
	readFileNum int64
	writeFileNum int64
	depth int64
	
	mutex sync.Mutex
	
	name string
	dataPath string
	maxBytesPerFile int64
	minMsgSize int64
	maxMsgSize int64
	syncEvery int64
	syncTimeout time.Duration
	
	nextReadPos int64
	nextReadFileNum int64
	
	readFile *os.File
	writeFile *os.File
	reader *bufio.Reader
	writeBuf bytes.Buffer
	
	readChan chan []byte
	writeResponseChan chan error
	emptyChan chan  int
	emptyResponseChan chan error
}

func NewDiskQueue() *DiskQueue {
	d := &DiskQueue{}
	
	return d
}

func (b *DiskQueue) Start()  {
	
}

func (b *DiskQueue) Put([]byte) error {
	return nil
}

func (b *DiskQueue) ReadChan() chan []byte {
	return nil
}

func (b *DiskQueue) Close() error {
	return nil
}

func (b *DiskQueue) Delete() error {
	return nil
}

func (b *DiskQueue) Depth() int64 {
	return 0
}

func (b *DiskQueue) Empty() error {
	return nil
}
