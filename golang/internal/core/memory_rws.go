package core

import "io"

// MemoryRws implements an io.ReadWriteSeeker using a byte slice for backing.
type MemoryRws struct {
	data []byte
	pos  int
}

func NewMemoryRws() *MemoryRws {
	return &MemoryRws{
		data: []byte{},
		pos:  0,
	}
}

// Read reads up to len(p) bytes into p. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered.
func (rws *MemoryRws) Read(p []byte) (n int, err error) {
	if p == nil {return 0, nil}

	readLen := imin(len(p), len(rws.data) - rws.pos) + rws.pos
	n = 0
	i := rws.pos
	for ; i < readLen; i++ {
		p[n] = rws.data[i]
		n++
	}
	rws.pos = i
	return n, nil
}

// Write writes len(p) bytes from p to the underlying data stream.
// It returns the number of bytes written from p (0 <= n <= len(p))
func (rws *MemoryRws) Write(p []byte) (n int, err error) {
	if p == nil {return 0, nil}

	writeEnd := len(p) + rws.pos
	extra := writeEnd - len(rws.data)
	if extra > 0 {
		rws.data = append(rws.data, make([]byte, extra)...)
	}
	n = 0
	i := rws.pos
	for ; i < writeEnd; i++ {
		rws.data[i] = p[n]
		n++
	}
	rws.pos = i
	return n, nil
}

// Seek sets the offset for the next Read or Write to offset, and returns the new offset
// relative to the start of the file and an error, if any.
func (rws *MemoryRws) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		rws.pos = int(offset)
	case io.SeekCurrent:
		rws.pos = imin(rws.pos+int(offset), len(rws.data))
	case io.SeekEnd:
		rws.pos = len(rws.data) + int(offset)
	default:
		panic("invalid seek type")
	}
	return int64(rws.pos), nil
}

func (rws *MemoryRws) Len() int64 {
	if rws == nil {return 0}
	return int64(len(rws.data))
}

func imin(a int, b int) int {if a < b {return a}; return b}