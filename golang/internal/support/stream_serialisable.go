package support

import "io"

type LengthReader interface {
	io.Reader
	Len() int
}

// StreamSerialisable is an interface for objects that can serialise to/from bytes
type StreamSerialisable interface {
	// Freeze converts to a byte stream
	Freeze() LengthReader

	// Defrost populates data from a byte stream
	Defrost(reader io.Reader) error
}