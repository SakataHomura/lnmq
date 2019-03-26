package qutils

type IdFactory struct {
	workerId      int64
	dataCenterId  int64
	lastTimestamp int64
	sequence      int64
}

var IdGenerater IdFactory

const (
	twepoch            int64 = int64(1420041600000)
	workerIdBits       int64 = int64(5)
	datacenterIdBits   int64 = int64(5)
	sequenceBits       int64 = int64(12)
	sequenceMask       int64 = int64(-1) ^ (int64(-1) << uint64(sequenceBits))
	workerIdShift      int64 = sequenceBits
	datacenterIdShift  int64 = sequenceBits + workerIdBits
	timestampLeftShift       = sequenceBits + workerIdBits + datacenterIdBits
)

func (f *IdFactory) NewGuid() int64 {
	timestamp := GetTimeMillis()

	if timestamp < f.lastTimestamp {
		timestamp = tilNextMillis(f.lastTimestamp)
	}

	if timestamp == f.lastTimestamp {
		f.sequence = (f.sequence + 1) & sequenceMask
		if f.sequence == 0 {
			timestamp = tilNextMillis(f.lastTimestamp)
		}
	} else {
		f.sequence = 0
	}

	f.lastTimestamp = timestamp

	return (((int64(timestamp) - twepoch) << uint64(timestampLeftShift)) |
		(f.dataCenterId << uint64(datacenterIdShift)) |
		(f.workerId << uint64(workerIdShift)) |
		f.sequence)
}
