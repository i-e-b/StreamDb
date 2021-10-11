package core

import (
	"golang/internal/structure"
	"errors"
	"fmt"
	"io"
)

// SimplePageStream represents a page chain (reverse order) as an io.ReadSeeker stream.
type SimplePageStream struct {
	parent    *PageStorage
	endPageId int
	length    int32 // length is total bytes in the stream
	position  int32
	cached    bool
	pageCache []*structure.BasicPage
}

// NewSimplePageStream represents a page chain (reverse order) as a read-only forward-order data stream.
func NewSimplePageStream(parent *PageStorage, endPageId int) *SimplePageStream {
	return &SimplePageStream{
		parent:    parent,
		endPageId: endPageId,
		length:    0,
		position:  0,
		cached:    false,
		pageCache: []*structure.BasicPage{},
	}
}

// Read consumes data from pages in forward-order.
func (stream *SimplePageStream)Read(p []byte) (n int, err error) {
	err = stream.loadPageIdCache()
	if err != nil {return 0, err}
	count := int32(len(p))
	var actual int

	pageIdx := int(stream.position / structure.PageDataCapacity)
	startingOffset := int32(stream.position % structure.PageDataCapacity)

	if pageIdx < 0 {return 0, errors.New("read started out of bounds of page chain")}
	if pageIdx >= len(stream.pageCache) {return 0, io.EOF} // ran off end

	remains := min(count, stream.length - stream.position)
	written := int32(0)

	for remains > 0 {
		page := stream.pageCache[pageIdx]
		if page == nil {return 0, fmt.Errorf("page %d lost between cache and read", pageIdx)}

		available := page.GetDataLength() - startingOffset
		if available < 1 {return 0, fmt.Errorf("read from page chain returned %d bytes available", available)}

		rdr := page.BodyReader()
		err = seekReader(rdr, startingOffset)
		if err != nil {return 0, err}

		request := min(available, count - written)
		if request < 1 {return 0, errors.New("read stalled")}
		if request + written > count {return 0, errors.New("read would overrun buffer")}

		try := make([]byte, request)
		actual, err = rdr.Read(try)
		if err != nil {return 0, err}
		if actual < 1 {return 0, io.EOF}

		o := int(written)
		for i := 0; i < actual; i++ {p[i+o] = try[i]}

		written += int32(actual)
		remains -= int32(actual)

		pageIdx++
		startingOffset = 0
	}

	if written < 1 {return 0, io.EOF}
	stream.position += written
	return int(written), nil
}

// Seek changes position in the page stream.
func (stream *SimplePageStream)Seek(offset int64, whence int) (int64, error){
	switch whence {
	case io.SeekStart:
		stream.position = int32(offset)
	case io.SeekCurrent:
		stream.position = min(stream.position+int32(offset), stream.length)
	case io.SeekEnd:
		stream.position = stream.length + int32(offset)
	default:
		panic("invalid seek type")
	}
	return int64(stream.position), nil
}

// GetLength returns the number of bytes in the stream.
func (stream *SimplePageStream)GetLength() int32 {
	err := stream.loadPageIdCache()
	if err != nil {panic(err)}
	return stream.length
}

// GetPosition returns the current read-head position relative to the stream start.
func (stream *SimplePageStream)GetPosition() int32 {
	err := stream.loadPageIdCache()
	if err != nil {panic(err)}
	return stream.position
}

func seekReader(rdr io.Reader, n int32) error {
	n64 := int64(n)
	actual, err := io.CopyN(io.Discard, rdr, n64)
	if err != nil {return err}
	if actual < n64 {return fmt.Errorf("seeking to %d moved only %d bytes", n, actual)}
	return nil
}

func min(a int32, b int32) int32 {
	if a < b {return a}
	return b
}

func (stream *SimplePageStream)loadPageIdCache() error {
	if stream.cached {return nil}

	var pageBytes int32 = 0
	p, err := stream.parent.GetRawPage(stream.endPageId)
	if err != nil {return err}
	for p != nil {
		stream.pageCache = append(stream.pageCache, p)
		pageBytes += p.GetDataLength()
		p, err = stream.parent.GetRawPage(int(p.GetPrevPageId()))
	}

	reversePageList(stream.pageCache)

	stream.length = pageBytes
	stream.cached = true
	return nil
}

func reversePageList(list []*structure.BasicPage) {
	end := len(list) - 1
	for i := 0; i < end; i++ {
		if i >= end {return}
		list[i], list[end] = list[end], list[i]
		end--
	}
}