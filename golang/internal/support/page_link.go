package support

import (
	"golang/internal/comparable"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

// pageLink is a versioned link to a page chain.
// These should always be used in pairs. The most recent is read, the older is overwritten.
type pageLink struct {
	Version *MonotonicByte
	PageId  int
}

// VersionedLink is a pair of versioned page links, and calls to read and update them.
type VersionedLink struct {
	A, B *pageLink
	lock *sync.Mutex
}

const VersionLinkByteSize = 10

func NewVersionedLink() *VersionedLink{
	out := VersionedLink{
		A:    newInvalidPageLink(),
		B:    newInvalidPageLink(),
		lock: &sync.Mutex{},
	}
	return &out
}

// TryGetLink tries to read the page id of a given revision
func (link *VersionedLink) TryGetLink(revision int) (ok bool, pageId int) {
	link.lock.Lock()
	defer link.lock.Unlock()

	pageId = -1
	if revision > 1 || revision < 0 {
		return false, pageId
	} // not supported
	if link.A.PageId < 0 && link.B.PageId < 0 {
		return false, pageId
	} // no versions

	if link.B.PageId < 0 { // B hasn't been written
		if revision == 0 {
			pageId = link.A.PageId
		} else {
			pageId = link.B.PageId
		}
		return pageId >= 0, pageId
	}

	if link.A.Version == link.B.Version {
		panic("versionedLink.TryGetLink: option table versions invalid")
	}

	if comparable.Is(link.A.Version).GreaterThan(link.B.Version) { // B is older
		if revision == 0 {
			pageId = link.A.PageId
		} else {
			pageId = link.B.PageId
		}
		return pageId >= 0, pageId
	}

	if revision == 0 {
		pageId = link.B.PageId
	} else {
		pageId = link.A.PageId
	}
	return pageId >= 0, pageId
}

// WriteNewLink tries to write a new page id, updating revisions and returning expired pages if any
func (link *VersionedLink) WriteNewLink(pageId int) (expiredPage int) {
	link.lock.Lock()
	defer link.lock.Unlock()
	expiredPage = -1

	if link.A.PageId < 0 {
		// A has never been set
		link.A = newPageLink(pageId, NewMonotonicByte(0))
		return expiredPage
	}

	if link.B.PageId < 0 {
		// B has never been set
		link.B = newPageLink(pageId, link.A.Version.Next())
		return expiredPage
	}

	if comparable.Is(link.A.Version).EqualTo(link.B.Version) {
		panic("VersionedLink.WriteNewLink: option table versions invalid")
	}

	if comparable.Is(link.A.Version).GreaterThan(link.B.Version) {
		// B is older. Replace it.
		expiredPage = link.B.PageId
		link.B = newPageLink(pageId, link.A.Version.Next())
		return expiredPage
	}

	// A is older. Replace it.
	expiredPage = link.A.PageId
	link.A = newPageLink(pageId, link.B.Version.Next())
	return expiredPage
}

// Freeze converts to a byte stream
func (link *VersionedLink) Freeze() LengthReader {
	link.lock.Lock()
	defer link.lock.Unlock()

	buf := bytes.NewBuffer([]byte{})

	writePageLinkToBuffer(link.A, buf)
	writePageLinkToBuffer(link.B, buf)

	return buf
}

// Defrost populates data from a byte stream
func (link *VersionedLink)Defrost(reader io.Reader) error {
	link.lock.Lock()
	defer link.lock.Unlock()

	if reader == nil {return errors.New("data source was invalid")}
	buf := make([]byte, VersionLinkByteSize)
	count, err := reader.Read(buf)
	if err != nil {return err}
	if count < VersionLinkByteSize {return errors.New("VersionedLink.Defrost: data was too short")}

	v1 := buf[0]

	p1 := binary.BigEndian.Uint32(buf[1:5])

	v2 := buf[5]
	p2 := binary.BigEndian.Uint32(buf[6:])

	link.A = newPageLink(int(int32(p1)), NewMonotonicByte(int(v1)))
	link.B = newPageLink(int(int32(p2)), NewMonotonicByte(int(v2)))
	return nil
}

// newPageLink returns a new link with a zeroed page id.
func newPageLink(pageId int, version *MonotonicByte) *pageLink {
	return &pageLink{
		Version: version,
		PageId:  pageId,
	}
}

// newInvalidPageLink returns a link that is disabled.
func newInvalidPageLink() *pageLink {
	return &pageLink{
		Version: NewMonotonicByte(0),
		PageId:  -1,
	}
}


func writePageLinkToBuffer(link *pageLink, buf *bytes.Buffer) {
	v1 := link.Version.Freeze()
	_, _ = io.Copy(buf, v1)
	writeIntToBuffer(link.PageId, buf)
}

func writeIntToBuffer(link int, buf *bytes.Buffer) {
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, uint32(link))
	buf.Write(tmp)
}