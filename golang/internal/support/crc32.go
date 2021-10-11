package support

const (
	DefaultPolynomial uint32 = 0xedb88320
	DefaultSeed uint32 = 0xffffffff
)

var (
	defaultTable []uint32
)

func init() {
	defaultTable = make([]uint32, 256)
	for i := 0; i < 256; i++ {
		entry := uint32(i)
		for j := 0; j < 8; j++ {
			if (entry & 1) == 1 {
				entry = (entry >> 1) ^ DefaultPolynomial
			} else {
				entry = entry >> 1
			}
		}
		defaultTable[i] = entry
	}
}

func ComputeCRC32(buffer []byte) uint32 {
	if buffer == nil {
		return 0
	}
	var crc = DefaultSeed
	for i := 0; i < len(buffer); i++ {
		b := uint32(buffer[i])
		crc = (crc >> 8) ^ defaultTable[b^(crc&0xff)]
	}
	return ^crc
}