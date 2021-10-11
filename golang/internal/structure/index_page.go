package structure

import (
	. "golang/internal/comparable"
	"golang/internal/support"
	"bytes"
	"errors"
	"io"
)


const (
	IndexEntryCount = 126 // 2+4+8+16+32+64
	IndexPackedSize = 3279 // (16+5+5) * 126

	same = 0
	less = -1
	greater = 1
)

var (
	zeroDocId = support.NewZeroId()
	neutralDocId = support.NewNeutralId()
)

// IndexPage holds the content of a single index page.
// The indexing system is a chained list of separate binary trees.
/*

   Layout: [ Doc Guid (16 bytes) | PageLink[0] (5 bytes) | PageLink[1] (5 bytes) ] --> 26 bytes
   We can fit 157 in a 4k page. Gives us 6 ranks (126 entries) -> 3276 bytes
   Our pages are currently 4061 bytes, so we have plenty of spare space if we can find useful metadata to store.

   We assume but DON'T store a root page with guid {127,127...,127}. The first two entries are 'left' and 'right' on the second level.

*/
type IndexPage struct {
	links []*support.VersionedLink
	docIds []*support.SerialId
}

func NewIndexPage() *IndexPage{
	page := IndexPage{
		links:  make([]*support.VersionedLink, IndexEntryCount),
		docIds: make([]*support.SerialId, IndexEntryCount),
	}
	for i := 0; i < IndexEntryCount; i++ {
		page.links[i] = support.NewVersionedLink()
		page.docIds[i] = support.NewZeroId()
	}

	return &page
}

func (page *IndexPage)TryInsert(docId *support.SerialId, pageId int) (bool,error) {
	index := find(page, docId)
	if index < 0 || index > IndexEntryCount {return false, nil} // no space left

	if Is(page.docIds[index]).NotEqual(zeroDocId) {return false, errors.New("tried to insert a duplicate document id")}

	// ok, we have a space
	_ = page.links[index].WriteNewLink(pageId)
	page.docIds[index] = docId
	return true, nil
}

// Search tries to find a link in this index page. Returns true if found, false if not found.
// If found, this will return up to two page-options. Use the newest one with a valid CRC in the page.
func (page *IndexPage)Search(docId *support.SerialId) (link *support.VersionedLink, found bool){
	index := find(page, docId)
	if index < 0 || index >= IndexEntryCount {return nil, false}
	if Is(page.docIds[index]).EqualTo(zeroDocId) {return nil, false}
	if Is(page.docIds[index]).NotEqual(docId) {panic("index page Search: logic error")}

	return page.links[index], true
}

// Update updates a link with a new PageID. The oldest link will be updated.
// Returns true if a change was made. False if the link was not found in this index page.
func (page *IndexPage)Update(docId *support.SerialId, pageId int) (expiredPage int, found bool){
	index := find(page, docId)
	if index < 0 || index >= IndexEntryCount {return -1, false}
	if Is(page.docIds[index]).EqualTo(zeroDocId) {return -1, false}
	if Is(page.docIds[index]).NotEqual(docId) {panic("index page Update: logic error")}

	expiredPage = page.links[index].WriteNewLink(pageId)
	return expiredPage, true
}

// Remove updates a link to set an invalid link. Both versions of the existing link will be lost.
// Returns true if a change was made. False if the link was not found in this index page
func(page *IndexPage) Remove(docId *support.SerialId) bool{
	index := find(page, docId)
	if index < 0 || index >= IndexEntryCount {return false}
	if Is(page.docIds[index]).EqualTo(zeroDocId) {return false}
	if Is(page.docIds[index]).NotEqual(docId) {panic("index page Remove: logic error")}

	page.links[index] = support.NewVersionedLink() // reset
	return true
}

// Freeze converts to a byte stream
func(page *IndexPage) Freeze() support.LengthReader {
	buf := bytes.NewBuffer([]byte{})

	for i := 0; i < IndexEntryCount; i++ {
		// Alternate doc-ids and page-version-links (both fixed size)
		id := page.docIds[i].Freeze()
		_, _ = io.Copy(buf, id)

		link := page.links[i].Freeze()
		_, _ = io.Copy(buf, link)
	}

	return buf
}

// Defrost populates data from a byte stream
func(page *IndexPage) Defrost(reader io.Reader) error{
	for i := 0; i < IndexEntryCount; i++ {
		id := support.NewZeroId()
		err := id.Defrost(reader)
		if err != nil {return err}
		page.docIds[i] = id

		link := support.NewVersionedLink()
		err = link.Defrost(reader)
		if err != nil {return err}
		page.links[i] = link
	}
	return nil
}

// find tries to find an entry index by a guid key. This is used in insert, search, update.
// If no such entry exists, but there is a space for it, you will get a valid index whose
// `docIds` entry is Guid.Zero -- so always check.
func find(page *IndexPage, target *support.SerialId) int {
	// the implicit node:
	cmpNode := neutralDocId
	leftIdx := 0
	rightIdx := 1

	current := -1

	// step down through the implicit binary tree
	for i := 0; i < 7; i++ {
		switch cmpNode.CompareTo(target) {
		case same: return current // found it!
		case less:
			// move left
			current = leftIdx

		case greater:
			// move right
			current = rightIdx

		default:
			panic("comparable returned unexpected value")
		}

		// update 'next' pointers
		leftIdx = (current*2) + 2
		rightIdx = (current*2) + 3

		// make sure we're still in bounds
		if current < 0 {panic("index tree TryInsert: Logic error")}
		if current >= IndexEntryCount {return -1}

		cmpNode = page.docIds[current]
		if Is(cmpNode).EqualTo(zeroDocId) {return current}
	}
	panic("index tree TryInsert: out of loop bounds")
}