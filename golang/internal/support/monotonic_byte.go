package support

import (
	"bytes"
	"errors"
	"io"
	"math"
)

// MonotonicByte is a counter that can count 'up' indefinitely and be compared to another
// as long as the two aren't too far apart.
type MonotonicByte struct {
	value byte
}

func NewMonotonicByte(value int) *MonotonicByte{
	val := MonotonicByte{
		value: byte(value),
	}
	return &val
}

// Next returns a new counter that is one version ahead of this one
func (counter *MonotonicByte)Next() *MonotonicByte{
	next := NewMonotonicByte(int(counter.value))
	next.Increment()
	return next
}

// Increment advances the counter one position
func (counter *MonotonicByte) Increment() byte {
	counter.value++
	return counter.value
}

func (counter *MonotonicByte) Freeze() LengthReader {
	return bytes.NewReader([]byte{counter.value})
}

func (counter *MonotonicByte) Defrost(reader io.Reader) error{
	buf := []byte{0}
	count, err := reader.Read(buf)
	if err != nil {return err}
	if count < 1 {return errors.New("reader did not contain enough data")}

	counter.value = buf[0]
	return nil
}

func (counter *MonotonicByte) CompareTo(other interface{}) int {
	otherCounter, ok := other.(*MonotonicByte)
	if !ok {
		oc2, ok2 := other.(MonotonicByte)
		if !ok2 {return 0}
		otherCounter = &oc2
	}

	diff := math.Abs(float64(otherCounter.value) - float64(counter.value))
	val := int(counter.value) - int(otherCounter.value)
	if diff > 63 {return 1-val}
	return val
}