package structure

import (
	"golang/internal/support"
	"bytes"
	"errors"
	"io"
)

// FreeListPage is the structure for the "free pages" list. Implements support.StreamSerialisable
/*
The free chain is a set of pages, each of which is just a big array of Int32 entries
page zero is always occupied, and negative pages are invalid, so either of these is an empty slot in the free list

Each free page can hold 1020 IDs of pages (3.9 MB of document data space) -- so having multiples *should* be rare
When searching for a free page, we scan the free chain first. If we can't find anything we
allocate more space (writing off the end of the stream).

Reading and writing free pages is done as close to the start of the chain as possible.
The first free chain page is never removed, but the other pages can be removed when empty.

Our database keeps up to 2 versions of each document, freeing pages as the third version 'expires',
so in applications where updates happen a lot, we expect the free chain to be busy

There is no clever data structures or algorithms here yet, it's just a scan.
The free list provides no protection from double-free. The caller should check the returned page
is not in use (with page type and document id).
 */
type FreeListPage struct {
	entries []int32
}

const (
	FreePageCapacity = PageDataCapacity / 4 // unsafe.Sizeof(int32), if that actually worked.
)

// NewFreeListPage makes a new empty free list page
func NewFreeListPage() *FreeListPage{
	return &FreeListPage{
		entries: make([]int32, FreePageCapacity),
	}
}

// TryGetNext returns a free page if it can be found.
// Returns false if no free pages are available.
// The returned free page will be removed from the list as part of the get call.
func (page *FreeListPage)TryGetNext() (int32,bool){
	id := int32(-1)
	for i := 0; i < FreePageCapacity; i++ {
		if page.entries[i] < 3 {continue} // 'special' pages are never free

		id = page.entries[i]
		page.entries[i] = 0
		return id, true
	}
	return -1, false
}

// TryAdd adds a new free page to the list. Returns true if it worked, false if there was no free space
func (page *FreeListPage)TryAdd(pageId int32) bool {
	if pageId < 3 {return false}
	for i := 0; i < FreePageCapacity; i++ {
		if page.entries[i] == pageId {
			return true // already freed
		}
		if page.entries[i] > 3{
			continue // not an empty slot
		}

		page.entries[i] = pageId
		return true
	}
	return false // this page is full
}

// Freeze converts to a byte stream
func (page *FreeListPage) Freeze() support.LengthReader {
	buf := make([]byte, FreePageCapacity*4)
	pos := 0

	for _, entry := range page.entries {
		pos = writeInt32(buf, pos, entry)
	}
	return bytes.NewReader(buf)
}

// Defrost populates data from a byte stream
func (page *FreeListPage)Defrost(reader io.Reader) error{
	for i := 0; i < len(page.entries); i++ {
		v, err := readInt32(reader)
		if err != nil {return err}
		page.entries[i] = v
	}
	return nil
}

func writeInt32(target []byte, pos int, value int32) int {
	target[pos+0] = byte((value >> 24) & 0xff)
	target[pos+1] = byte((value >> 16) & 0xff)
	target[pos+2] = byte((value >> 8) & 0xff)
	target[pos+3] = byte((value >> 0) & 0xff)
	return pos + 4
}

func readInt32(reader io.Reader) (int32,error) {
	buf := make([]byte, 4)
	read, err := reader.Read(buf)
	if err != nil {return 0, err}
	if read != 4 {return 0, errors.New("defrost stream was truncated")}
	val := (int32(buf[0]) << 24) +
		   (int32(buf[1]) << 16) +
		   (int32(buf[2]) << 8) +
		   (int32(buf[3]) << 0)

	return val, nil
}