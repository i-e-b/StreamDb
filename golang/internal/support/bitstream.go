package support

import (
	"errors"
	"io"
)

type BitStream struct {
	base       io.ReadWriter
	runoutBits int

	inRunOut            bool
	readMask, writeMask byte
	nextOut, currentIn  int
}

func BitwiseStreamWrapper(source io.ReadWriter, runoutBits int) *BitStream {
	bs := BitStream{
		base:       source,
		runoutBits: runoutBits,
		inRunOut:   false,
		readMask:   0x01,
		writeMask:  0x80,
		nextOut:    0,
		currentIn:  0,
	}
	return &bs
}

// Flush writes any waiting bits as a full byte to the
// underlying stream
func (bs *BitStream) Flush() error {
	if bs.writeMask == 0x80 { // nothing to flush
		return nil
	}

	_, err := bs.base.Write([]byte{byte(bs.nextOut)})
	if err != nil {
		return err
	}

	bs.writeMask = 0x80
	bs.nextOut = 0
	return nil
}

// WriteBit writes a bit to the stream, with value = 0 giving a 0 bit,
// and any other value gives a 1 bit.
func (bs *BitStream) WriteBit(value int) error {
	if value != 0 {
		bs.nextOut = bs.nextOut | int(bs.writeMask)
	}
	bs.writeMask >>= 1

	if bs.writeMask == 0 {
		_, err := bs.base.Write([]byte{byte(bs.nextOut)})
		if err != nil {
			return err
		}

		bs.writeMask = 0x80
		bs.nextOut = 0
	}
	return nil
}

// ReadBit returns a 1 or 0 value from the stream.
func (bs *BitStream) ReadBit() (int, error) {
	if bs.inRunOut {
		bs.runoutBits--
		if bs.runoutBits >= 0 {
			return 0, nil
		}
		return 0, errors.New("end of input stream")
	}

	if bs.readMask == 1 {
		buf := []byte{0}
		n, err := bs.base.Read(buf)
		if err != nil {
			if err != io.EOF {
				return 0, err
			}
		}

		bs.currentIn = int(buf[0])
		if n <= 0 {
			bs.inRunOut = true
			bs.runoutBits--
			if bs.runoutBits >= 0 {
				return 0, nil
			}
			return 0, errors.New("end of input stream")
		}
		bs.readMask = 0x80
	} else {
		bs.readMask >>= 1
	}
	if bs.currentIn&int(bs.readMask) == 0 {
		return 0, nil
	}
	return 1, nil
}

// TryReadBit will read a bit from the stream, returning ok=false
// if data is not available.
func (bs *BitStream) TryReadBit() (b int, ok bool) {
	bit, err := bs.ReadBit()
	if err != nil {
		return 0, false
	}
	return bit, true
}

// IsEmpty returns true when the source bits are exhausted (excludes run-out)
func (bs *BitStream) IsEmpty() bool {
	return bs.inRunOut
}

// EncodeUint writes a number in a compact number encoding that maintains byte alignment.
func (bs *BitStream) EncodeUint(value uint32) error {
	if value < 127 { // one byte (7 data bits)
		err := bs.WriteBit(0); if err != nil {return err}
		for i := 0; i < 7; i++ {
			err = bs.WriteBit((int) ((value >> i) & 1))
			if err != nil {return err}
		}
		return nil
	}

	value -= 127
	if value < 16384 { // two bytes (14 bits data)
		err := bs.WriteBit(1); if err != nil {return err}
		err = bs.WriteBit(0); if err != nil {return err}
		for i := 0; i < 14; i++ {
			err = bs.WriteBit((int) ((value >> i) & 1));
			if err != nil {return err}
		}
		return nil
	}

	value -= 16384

	// Otherwise, 3 bytes (22 bit data)
	err := bs.WriteBit(1); if err != nil {return err}
	err = bs.WriteBit(1); if err != nil {return err}
	for i := 0; i < 22; i++ {
		err = bs.WriteBit((int)((value >> i) & 1))
		if err != nil {return err}
	}
	return nil
}

// TryDecodeUint reads a value previously written with EncodeUint.
func (bs *BitStream) TryDecodeUint() (value uint32, ok bool) {
	value = 0
	bit, ok := bs.TryReadBit()
	if !ok {return 0, false}

	if bit == 0 { // one byte (7 bits data)
		for i := 0; i < 7; i++ {
			b,ok_ := bs.TryReadBit(); if !ok_ {return 0,false}
			value |= uint32(b << i)
		}
		return value, true
	}

	bit, ok = bs.TryReadBit()
	if !ok {return 0, false}
	if bit == 0 { // two byte (14 bits data)
		for i := 0; i < 14; i++ {
			b,ok_ := bs.TryReadBit(); if !ok_ {return 0,false}
			value |= uint32(b << i)
		}
		value += 127
		return value, true
	}

	// 3 bytes (22 bit data)
	for i := 0; i < 22; i++ {
		b,ok_ := bs.TryReadBit(); if !ok_ {return 0,false}
		value |= uint32(b << i)
	}
	value += 16384 + 127
	return value, true
}