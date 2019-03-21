package qutils

type IdFactory struct {
    workerId uint64
    dataCenterId uint64
    lastTimestamp int64
    sequence uint64
}

const (
    twepoch uint64 = uint64(1420041600000)
    workerIdBits uint64 = uint64(5)
    datacenterIdBits uint64 = uint64(5)
    sequenceBits uint64 = uint64(12)
    sequenceMask uint64 = uint64(-1) ^ (uint64(-1) << sequenceBits)
    workerIdShift uint64 = sequenceBits
    datacenterIdShift uint64 = sequenceBits + workerIdBits
    timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits
)

func (f *IdFactory) NewGuid() uint64 {
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

    return (((uint64(timestamp) - twepoch) << timestampLeftShift) |
        (f.dataCenterId << datacenterIdShift) |
        (f.workerId << workerIdShift) |
        f.sequence)
}
