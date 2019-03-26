package qbackend

type EmptyBackendQueue struct {
}

func (b *EmptyBackendQueue) Put([]byte) error {
	return nil
}

func (b *EmptyBackendQueue) ReadChan() chan []byte {
	return nil
}

func (b *EmptyBackendQueue) Close() error {
	return nil
}

func (b *EmptyBackendQueue) Delete() error {
	return nil
}

func (b *EmptyBackendQueue) Depth() int64 {
	return 0
}

func (b *EmptyBackendQueue) Empty() error {
	return nil
}
