package support

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
)

// SerialId is a randomised identifier that implements the StreamSerialisable and Comparable interface.
// It is a bit like a uuid, but it's not.
type SerialId struct {
	value []byte
}

// NewZeroId creates a new SerialId with an empty value.
// NewRandomId is guaranteed never to return the empty id.
func NewZeroId() *SerialId{
	u := new([16]byte)
	out := SerialId{value: u[:]}
	return &out
}

// NewNeutralId returns an id whose bytes are all = 127.
// NewRandomId is guaranteed never to return the neutral id.
func NewNeutralId() *SerialId{
	return &SerialId{value: []byte{127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127}}
}

// NewRandomId creates a new SerialId with a random value.
// This will never return the Zero id, nor the Neutral id.
func NewRandomId() (*SerialId, error){
	u := new([16]byte)
	// Set all bits to pseudo-randomly chosen values.
	_, err := rand.Read(u[:])
	if err != nil {
		return nil, err
	}
	u[15] = 240
	out := SerialId{value: u[:]}
	return &out, nil
}

func (id *SerialId) CompareTo(other interface{}) int {
	otherId, ok := other.(*SerialId)
	if !ok {
		oc2, ok2 := other.(SerialId)
		if !ok2 {return 0}
		otherId = &oc2
	}

	// bytes are compared [0] -> most-significant, [15] -> least-significant
	for i := 0; i < 16; i++ {
		a := id.value[i]
		b := otherId.value[i]
		cmp := int(a) - int(b)
		if cmp < 0 {return -1}
		if cmp > 0 { return 1 }
	}
	return 0
}

// Freeze converts to a byte stream
func (id *SerialId) Freeze() LengthReader {
	return bytes.NewReader(id.value)
}

// Defrost populates data from a byte stream
func (id *SerialId)Defrost(reader io.Reader) error {
	count, err := reader.Read(id.value)
	if err != nil {return err}
	if count != 16 {
		return errors.New("invalid id length")
	}
	return nil
}