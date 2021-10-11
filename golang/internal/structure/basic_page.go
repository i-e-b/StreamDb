package structure

import (
	"golang/internal/support"
	"bytes"
	"errors"
	"io"
)

// QuickAndDirtyMode flag. If set to true, the CRC checks on read are skipped.
// This results in faster reads, but corruption won't be found. Does not affect writes.
var QuickAndDirtyMode = false

const (
	// PageRawSize is the size of a page in storage, including all headers and data
	PageRawSize = 4096 // 4k data, to fit in a typical VM page

	// PageHeadersSize is the size of all metadata stored in a page
	PageHeadersSize = 12

	// PageDataCapacity is the maximum data capacity of a page
	PageDataCapacity = PageRawSize - PageHeadersSize

	// MaxInt32Index is the maximum page index that can be used
	MaxInt32Index = (PageDataCapacity / 4) - 1
)

/*

    bits   bytes    Data layout:
      32       4    [CRC32:       int32] <-- CRC of the entire page (including headers)
      64       8    [Length:      int32] <-- length of data stored in body
      96      12    [Prev:       uint32] <-- previous page in the sequence ( -1 if this is the start )
   32768    4096    [data:   byte[4084]] <-- page contents (interpret based on PageType)

*/
const (
	crcHash  = 0
	dataLen  = 4
	prevLnk  = 8
	pageData = 12
)


// BasicPage represents a general purpose page in the DB. These are fixed size.
// Implements support.StreamSerialisable.
type BasicPage struct {
	// data is the page data exactly as read or written to storage.
	data       []byte

	// PageId is the ID that this instance was loaded from. This is not written to storage.
	PageId     int
}

func NewBasicPage(pageId int) *BasicPage{
	page := &BasicPage{
		data:       make([]byte, PageRawSize),
		PageId:     pageId,
	}
	page.SetPrevPageId(-1)
	page.SetDataLength(0)

	return page
}

func (page *BasicPage) SetPrevPageId(v int32) { page.writeInt32(prevLnk, v) }
func (page *BasicPage) SetDataLength(v int32) { page.writeInt32(dataLen, v) }
func (page *BasicPage) SetCrcHash(v uint32)   { page.writeUint32(crcHash, v) }

func (page *BasicPage) GetPrevPageId() int32 { return page.readInt32(prevLnk) }
func (page *BasicPage) GetDataLength() int32 { return page.readInt32(dataLen) }
func (page *BasicPage) GetCrcHash() uint32   { return page.readUint32(crcHash) }

// UpdateCrc resets the CRC checksum based on current data
func (page *BasicPage) UpdateCrc(){
	page.SetCrcHash(0)
	newSum := support.ComputeCRC32(page.data)
	page.SetCrcHash(newSum)
}

// ValidateCrc checks the stored checksum against stored data. Returns true if data still matches sum.
func (page *BasicPage) ValidateCrc() bool {
	//goland:noinspection GoBoolExpressions
	if QuickAndDirtyMode {return true}

	stored := page.GetCrcHash()
	page.SetCrcHash(0)
	computed := support.ComputeCRC32(page.data)
	page.SetCrcHash(stored)

	ok := stored == computed
	return ok
}

// Write copies data from a buffer into the data section of the page
func (page *BasicPage) Write(input io.Reader, pageOffset, length int) error {
	if input == nil {return errors.New("invalid input")}
	if pageOffset + length > PageDataCapacity {
		return errors.New("page write exceeds page size")
	}

	buf := make([]byte, length)
	actual, err := input.Read(buf)
	if err != nil {return err}
	writeExtent := pageOffset + actual

	pageBase := pageData + pageOffset
	for i := 0; i < length; i++ {
		page.data[pageBase + i] = buf[i]
	}

	page.SetDataLength(int32(writeExtent))
	return nil
}

// ZeroAllData sets all content bytes to zero. Header values are not changed.
func (page *BasicPage) ZeroAllData(){
	for i := pageData; i < len(page.data); i++ {
		page.data[i] = 0
	}
}

// BodyReader returns an io.Reader over this single page's data.
func (page *BasicPage) BodyReader() io.Reader{
	end := pageData + page.GetDataLength()
	return bytes.NewReader(page.data[pageData:end])
}

// ReadDataInt32 treats the page DATA as an array of Int32. Reads from an index.
func (page *BasicPage) ReadDataInt32(idx int) (int32, error){
	if idx < 0 || idx > MaxInt32Index {return 0, errors.New("index out of range")}
	baseAddr := pageData + (idx * 4)
	return page.readInt32(baseAddr), nil
}

func (page *BasicPage) WriteDataInt32(idx int, value int32) error{
	if idx < 0 || idx > MaxInt32Index {return errors.New("index out of range")}
	baseAddr := pageData + (idx * 4)
	page.writeInt32(baseAddr, value)
	return nil
}

func (page *BasicPage) Freeze() support.LengthReader {
	return bytes.NewBuffer(page.data)
}
func (page *BasicPage) Defrost(reader io.Reader) error {
	length, err := reader.Read(page.data)
	if err != nil {
		return err
	}
	if length < PageRawSize {
		return errors.New("source was not long enough to fill a whole page")
	}
	return nil
}
func (page *BasicPage) readInt32(pos int) int32 {
	return (int32(page.data[pos+0]) << 24) + (int32(page.data[pos+1]) << 16) + (int32(page.data[pos+2]) << 8) + (int32(page.data[pos+3]) << 0)
}
func (page *BasicPage) readUint32(pos int) uint32 {
	return (uint32(page.data[pos+0]) << 24) + (uint32(page.data[pos+1]) << 16) + (uint32(page.data[pos+2]) << 8) + (uint32(page.data[pos+3]) << 0)
}
func (page *BasicPage) writeInt32(pos int, value int32) {
	page.data[pos+0] = byte((value >> 24) & 0xff)
	page.data[pos+1] = byte((value >> 16) & 0xff)
	page.data[pos+2] = byte((value >> 8) & 0xff)
	page.data[pos+3] = byte((value >> 0) & 0xff)
}
func (page *BasicPage) writeUint32(pos int, value uint32) {
	page.data[pos+0] = byte((value >> 24) & 0xff)
	page.data[pos+1] = byte((value >> 16) & 0xff)
	page.data[pos+2] = byte((value >> 8) & 0xff)
	page.data[pos+3] = byte((value >> 0) & 0xff)
}

func PageCountRequired(byteLength int64) int {
	full := byteLength / PageDataCapacity
	spare := byteLength % PageDataCapacity
	if spare > 0 {full++}
	return int(full)
}