package core

import (
	"golang/internal/structure"
	"golang/internal/support"
	"bytes"
	"errors"
	"io"
)

// PageStorage uses only reverse links and a streaming interface to implement a database over
// a seekable reader/writer.
// The start of this database is not a full page, but a set of versioned links out to:
//   - an index page chain
//   - a path-lookup page chain
//   - a free-list page chain
// All page version links point to the end of a chain.
// Pages cannot be updated in this store -- write a new copy and release the old one.
// This handles its free page list directly and internally.
// The main index and path lookup are normal documents with no special position.
type PageStorage struct {
	//lock            *sync.RWMutex // NOTE: single thread only in this port. Need a better lock as Go mutexes don't support lock recursion.
	file            io.ReadWriteSeeker
	pathLookupCache *support.ReverseTrie // of support.SerialId
	sync            func(io.ReadWriteSeeker) // this should sync the file with disk if possible
}
func trieCtor()support.SerialComparable{ return support.NewZeroId()}

const (
	// PSMagicSize is the number of bytes in our header
	PSMagicSize = 8

	// PSHeaderSize is the number of bytes in the 'hot' header
	PSHeaderSize = (support.VersionLinkByteSize * 3) + PSMagicSize

	pageIdxIndex = 0
	pageIdxPathLookup = 1
	pageIdxFreeList = 2

	badPage = -1
)

var (
	PSHeaderMagic = []byte{0x55, 0xAA, 0xFE, 0xED, 0xFA, 0xCE, 0xDA, 0x7A }
)

func NewPageStorage(fs io.ReadWriteSeeker, syncFunc func(io.ReadWriteSeeker)) (*PageStorage, error){
	if fs == nil {return nil, errors.New("invalid fs")}
	page := &PageStorage{
		file:            fs,
		sync:            syncFunc,
		pathLookupCache: nil,
	}

	size, err := fs.Seek(0, io.SeekEnd)
	if err != nil {panic(err)}

	if size == 0 {
		err = page.InitialiseDb()
		if err != nil {return nil, err}
		return page, nil
	}

	if size < PSHeaderSize {
		return nil, errors.New("stream is not empty, but is to short to read header information")
	}

	_, err = fs.Seek(0, io.SeekStart)
	if err != nil {return nil, err}

	check := make([]byte, PSMagicSize)
	count, err := fs.Read(check)
	if err != nil {return nil, err}
	if count < PSMagicSize {return nil, errors.New("file is too short to read header")}
	for i := 0; i < PSMagicSize; i++ {
		if check[i] != PSHeaderMagic[i] {
			return nil, errors.New("header did not match. Not a valid file")
		}
	}
	return page, nil
}

// InitialiseDb sets up a new database with the basic required headers
func (store *PageStorage) InitialiseDb() error{
	_, err := store.file.Seek(0, io.SeekStart)
	if err != nil {return err}

	// Write header magic
	_, err = store.file.Write(PSHeaderMagic)
	if err != nil {return err}

	// Write disabled links for the three core chains
	indexVersion := support.NewVersionedLink().Freeze()
	pathLookupVersion := support.NewVersionedLink().Freeze()
	freeListVersion := support.NewVersionedLink().Freeze()

	_, err = io.Copy(store.file, indexVersion)
	if err != nil {return err}

	_, err = io.Copy(store.file, pathLookupVersion)
	if err != nil {return err}

	_, err = io.Copy(store.file, freeListVersion)
	if err != nil {return err}

	store.sync(store.file)
	return nil
}

// GetStream returns a read-only page stream for a page chain, given its end-of-chain page id.
func (store *PageStorage) GetStream(endPageId int) *SimplePageStream {
	return NewSimplePageStream(store, endPageId)
}

// WriteStream stores a data stream from its current position to end into a new page chain. Returns the end page ID.
// This ID should then be stored either inside the index document, or to one of the core versions.
func (store *PageStorage) WriteStream(dataSource io.Reader) (endPageId int, err error){
	if dataSource == nil {return -1, errors.New("cannot write nil data source")}

	data, err := io.ReadAll(dataSource) // it would be nice to have a length on a stream.
	if err != nil {return -1, err}

	pagesRequired := structure.PageCountRequired(int64(len(data)))
	pages := make([]int, pagesRequired)
	err = store.allocatePageBlock(pages)
	if err != nil {return 0, err}

	return store.writeStreamInternal(data, pages, pagesRequired)
}

// CommitPage writes a page from memory to storage. This will update the CRC before writing.
func (store *PageStorage)CommitPage(page *structure.BasicPage) error {
	if page == nil {return errors.New("cannot commit a nil page")}
	if page.PageId < 0 {return errors.New("page id must be valid")}

	pageId := page.PageId
	page.UpdateCrc()

	//store.lock.Lock()
	//defer store.lock.Unlock()

	pageData := page.Freeze()
	_, err := store.file.Seek(getPagePhysicalLocation(pageId), io.SeekStart)
	if err != nil {return err}

	_, err = io.Copy(store.file, pageData)
	if err != nil {return err}
	store.sync(store.file)
	return nil
}

// ReleaseChain releases all pages in a chain. They can be reused on next write.
// If the page ID given is invalid, the release command is silently ignored
func (store *PageStorage)ReleaseChain(endPageId int) (released int, err error) {
	if endPageId < 0 {return released, nil}
	released = 0

	pagesSeen := set{}
	chainToRelease, err := store.GetRawPage(endPageId)
	if err != nil {return released, err}

	for chainToRelease !=nil {
		if pagesSeen.contains(chainToRelease.PageId){
			return released, errors.New("loop found in chain")
		}
		pagesSeen.set(chainToRelease.PageId)

		err = store.releaseSinglePage(chainToRelease.PageId) // this should add new pages as required
		if err != nil {return released, err}
		released++

		nextPage := chainToRelease.GetPrevPageId()
		chainToRelease, err = store.GetRawPage(int(nextPage))
		if err != nil {return released, err}
	}
	return released, nil
}

// BindIndex maps a document id to a page id. The page id should be the end of chain.
// If the document has an existing page, the versions will be incremented.
// If a version expires, the page id will be returned.
func (store *PageStorage)BindIndex(documentId *support.SerialId, newPageId int) (expiredPageId int, err error){
	// TODO: should be locking here? Migrate to channel & monitor

	var found bool
	indexLink := store.GetIndexPageLink()
	ok, indexTopPageId := indexLink.TryGetLink(0)
	if !ok {
		indexTopPageId = -1
	}

	// try to update an existing document
	currentPage, err := store.GetRawPage(indexTopPageId)
	if err != nil {return badPage, err}
	for currentPage != nil { // walk back through page links
		indexSnap := structure.NewIndexPage() // take a snapshot of this index page
		err = indexSnap.Defrost(currentPage.BodyReader())
		if err != nil {return badPage, err}

		expiredPageId, found = indexSnap.Update(documentId, newPageId)
		if found { // got an existing link for this doc. Update and save.
			return expiredPageId, writeIndexToStore(indexSnap, currentPage, store)
		}

		// didn't find the document, walk down the chain
		currentPage, err = store.GetRawPage(int(currentPage.GetPrevPageId()))
		if err != nil {return badPage, err}
	}

	// Didn't find an existing entry. Try to insert in an existing index page
	expiredPageId = -1
	currentPage, err = store.GetRawPage(indexTopPageId)
	if err != nil {return badPage, err}
	for currentPage != nil {
		indexSnap := structure.NewIndexPage() // take a snapshot of this index page
		err = indexSnap.Defrost(currentPage.BodyReader())
		if err != nil {return badPage, err}

		found, err = indexSnap.TryInsert(documentId, newPageId)
		if err != nil {return badPage, err}
		if found { // successfully added
			return expiredPageId, writeIndexToStore(indexSnap, currentPage, store)
		}

		// no room to add, go to next page
		currentPage, err = store.GetRawPage(int(currentPage.GetPrevPageId()))
		if err != nil {return badPage, err}
	}

	// Couldn't find any space in existing index pages.
	// Need to extend index chain and write new version of the head.
	newIndex := structure.NewIndexPage()
	ok, err = newIndex.TryInsert(documentId, newPageId)
	if err != nil || !ok {panic("failed to write to new index page")}

	slot := make([]int, 1)
	err = store.allocatePageBlock(slot)
	if err != nil {return badPage, err}

	newPage, err := store.GetRawPage(slot[0])
	if err != nil {return badPage, err}
	newPage.SetPrevPageId(int32(indexTopPageId))
	rdr := newIndex.Freeze()
	err = newPage.Write(rdr, 0, rdr.Len())
	if err != nil {return badPage, err}

	err = store.CommitPage(newPage)
	if err != nil {return badPage, err}

	// Set the new page as the index page link
	link := store.GetIndexPageLink()
	link.WriteNewLink(newPage.PageId)
	store.SetIndexPageLink(link)
	store.Flush()
	return expiredPageId, nil
}

// UnbindIndex removes a mapping from a document ID.
// The page chain is not affected. If no such document id is bound, nothing happens.
func (store *PageStorage)UnbindIndex(documentId *support.SerialId) error {
	// TODO: should be locking here? Migrate to channel & monitor

	indexLink := store.GetIndexPageLink()
	ok, indexTopPageId := indexLink.TryGetLink(0)
	if !ok {
		return nil // nothing to un-bind
	}

	currentPage, err := store.GetRawPage(indexTopPageId)
	if err != nil {return err}

	for currentPage != nil {
		indexSnap := structure.NewIndexPage()
		err = indexSnap.Defrost(currentPage.BodyReader())
		if err != nil {return err}

		found := indexSnap.Remove(documentId)
		if found { // found and removed from this page. Save and exit.
			err = writeIndexToStore(indexSnap, currentPage, store)
			if err != nil {return err}
		}

		currentPage, err = store.GetRawPage(int(currentPage.GetPrevPageId())) // keep walking
	}
	return nil
}

// GetDocumentHead finds the top page id for a given document id, by
// reading the index. If the document can't be found, returns -1.
func (store *PageStorage)GetDocumentHead(documentId *support.SerialId) (headPageId int, err error){
	indexLink := store.GetIndexPageLink()
	ok, indexTopPageId := indexLink.TryGetLink(0)
	if !ok {
		indexTopPageId = -1
	}

	currentPage,err := store.GetRawPage(indexTopPageId)
	if err != nil {return badPage, err}

	for currentPage != nil {
		indexSnap := structure.NewIndexPage()
		err = indexSnap.Defrost(currentPage.BodyReader())
		if err != nil {return badPage, err}

		link, found := indexSnap.Search(documentId)
		if found && link != nil {
			ok, headPageId = link.TryGetLink(0)
			if ok { return headPageId, nil}
		}

		currentPage, err = store.GetRawPage(int(currentPage.GetPrevPageId()))
		if err != nil {return badPage, err}
	}
	return badPage, nil
}

// BindPath links an exact path to a document ID.
// If an existing document was bound to the same path, its ID will be returned
func (store *PageStorage)BindPath(path string, documentId *support.SerialId)(previousDocId *support.SerialId, err error){
	if path == "" {
		return nil, errors.New("path must not be empty")
	}

	// Load path from storage (or start with an empty one)
	pathLink := store.GetPathLookupLink()
	pathIndex := support.NewReverseTrie(trieCtor)
	ok, pathPageId := pathLink.TryGetLink(0)
	if ok {
		err = pathIndex.Defrost(store.GetStream(pathPageId))
		if err != nil {return nil, err}
	}

	// Bind the path
	serial, err := pathIndex.Add(path, documentId)
	if err != nil {return nil, err}
	if serial != nil {previousDocId = serial.(*support.SerialId)}

	// Write back updated look-up to new chain
	newPageId, err := store.WriteStream(pathIndex.Freeze())
	if err != nil {return nil, err}

	// Update version link
	expired := pathLink.WriteNewLink(newPageId)
	store.SetPathLookupLink(pathLink)

	_, err = store.ReleaseChain(expired)
	if err != nil {return nil, err}

	store.Flush()
	return previousDocId, nil
}

// UnbindPath removes a path binding if it exists. If the path is not bound, nothing happens.
// Linked documents are not removed.
func (store *PageStorage)UnbindPath(exactPath string) error {
	pathLink := store.GetPathLookupLink()
	pathIndex := support.NewReverseTrie(trieCtor)

	ok, pathPageId := pathLink.TryGetLink(0)
	if !ok {return nil}

	err := pathIndex.Defrost(store.GetStream(pathPageId))
	if err != nil {return err}

	// Unbind the path
	changed := pathIndex.Delete(exactPath)
	if !changed {return nil} // no changes

	// Write updated paths
	newPageId, err := store.WriteStream(pathIndex.Freeze())
	if err != nil {return err}

	expired := pathLink.WriteNewLink(newPageId)
	store.SetPathLookupLink(pathLink)

	_, err = store.ReleaseChain(expired)
	if err != nil {return err}

	store.Flush()
	return nil
}

// GetDocumentIdByPath reads the path lookup, and returns the DocumentID stored at the exact path.
// Returns null if there is not document stored.
func (store *PageStorage)GetDocumentIdByPath(exactPath string) (docId *support.SerialId, err error){
	pathIndex := store.getPathLookupIndex()
	if pathIndex == nil {return nil, errors.New("could not find path lookup")}
	value, found, err := pathIndex.Get(exactPath)
	if err != nil {return nil, err}
	if !found {return nil,nil}
	return value.(*support.SerialId), nil
}

// GetPathsForDocument returns all paths currently bound for the given document ID.
// If no paths are bound, an empty slice is given.
func (store *PageStorage)GetPathsForDocument(docId *support.SerialId) (paths []string, err error){
	pathIndex := store.getPathLookupIndex()
	if pathIndex == nil {return nil, errors.New("could not find path lookup")}

	return pathIndex.GetPathsForEntry(docId), nil
}

// SearchPaths returns all paths currently bound that start with the given prefix.
// The prefix should not be null or empty. If no paths are bound, an empty enumeration is given.
func (store *PageStorage)SearchPaths(pathPrefix string) (paths []string){
	pathIndex := store.getPathLookupIndex()
	return pathIndex.Search(pathPrefix)
}

// GetRawPage reads a page from the storage stream to memory. This will check the CRC.
func (store *PageStorage)GetRawPage(pageId int) (*structure.BasicPage, error) {
	if pageId < 0 {return nil, nil}

	//store.lock.Lock()
	//defer store.lock.Unlock()

	_, err := store.file.Seek(getPagePhysicalLocation(pageId), io.SeekStart)
	if err != nil {return nil,err}

	result := structure.NewBasicPage(pageId)
	err = result.Defrost(store.file)
	if err != nil {return nil,err}

	if !result.ValidateCrc() {return nil, errors.New("page crc check failed")}

	return result, nil
}

// GetFreeListLink returns versioned links to the free-list pages
func (store *PageStorage) GetFreeListLink() *support.VersionedLink{
	return store.getLink(pageIdxFreeList)
}
// SetFreeListLink updates the versioned links to the free-list pages
func (store *PageStorage) SetFreeListLink(link *support.VersionedLink) {
	store.setLink(pageIdxFreeList, link)
}

// GetIndexPageLink returns versioned links to the free-list pages
func (store *PageStorage) GetIndexPageLink() *support.VersionedLink{
	return store.getLink(pageIdxIndex)
}
// SetIndexPageLink updates the versioned links to the free-list pages
func (store *PageStorage) SetIndexPageLink(link *support.VersionedLink) {
	store.setLink(pageIdxIndex, link)
}

// GetPathLookupLink returns versioned links to the path-lookup pages
func (store *PageStorage) GetPathLookupLink() *support.VersionedLink{
	return store.getLink(pageIdxPathLookup)
}
// SetPathLookupLink updates the versioned links to the path-lookup pages
func (store *PageStorage) SetPathLookupLink(link *support.VersionedLink) {
	store.setLink(pageIdxPathLookup, link)
}

// Flush writes any changed data to storage
func (store *PageStorage) Flush() {
	store.sync(store.file)
}


type set map[int]int
func (m set) contains(key int) bool{
	_, found := m[key]
	return found
}
func (m set) set(key int) {m[key] = key }

func (store *PageStorage) getPathLookupIndex() *support.ReverseTrie{
	if store.pathLookupCache != nil {return store.pathLookupCache}

	pathLink := store.GetPathLookupLink()
	pathIndex := support.NewReverseTrie(trieCtor)
	ok, pathPageId := pathLink.TryGetLink(0)
	if !ok {return nil}
	err := pathIndex.Defrost(store.GetStream(pathPageId))
	if err != nil {return nil}

	store.pathLookupCache = pathIndex
	return store.pathLookupCache
}

// releaseSinglePage adds a single page to release chain. This will create free list pages as required.
func (store *PageStorage) releaseSinglePage(pageToReleaseId int) error {
	// Note: if we need to extend the free list, we should use the last page in the current list.
	// So, we can't assume pages are full based on prevPageId value.

	freeLink := store.GetFreeListLink()
	hasList, topPageId := freeLink.TryGetLink(0)
	if !hasList { // create and set new free list page
		slot := []int{0}
		store.directlyAllocatePages(slot, 0)
		_ = freeLink.WriteNewLink(slot[0])
		topPageId = slot[0]
		store.SetFreeListLink(freeLink)
		store.Flush()
	}

	// Structure of free pages' data (see also `ReassignReleasedPages`)
	// [Entry count: int32] -> n
	// n * [PageId: int32]

	currentPage, err := store.GetRawPage(topPageId)
	if err != nil {return err}

	for currentPage != nil {
		length, err := currentPage.ReadDataInt32(0)
		if err != nil {return err}

		if length < structure.MaxInt32Index { // space remaining. Write value and exit
			length++
			err = currentPage.WriteDataInt32(int(length), int32(pageToReleaseId))
			if err != nil {return err}
			err = currentPage.WriteDataInt32(0, length)
			if err != nil {return err}

			err = store.CommitPage(currentPage)
			if err != nil {return err}

			return nil
		}
		// Else, no space here - walk along the chain

		prev := currentPage.GetPrevPageId()
		if prev >= 0 {
			currentPage, err = store.GetRawPage(int(prev))
			if err != nil {return err}
		} else { // no room in any of the free pages, need a new one
			// Use one out of the previous free-page-list
			newFreePage, err := store.GetRawPage(pageToReleaseId)
			if err != nil {return err}
			newFreePage.ZeroAllData()
			newFreePage.SetPrevPageId(-1)
			err = store.CommitPage(newFreePage)
			if err != nil {return err}
			currentPage.SetPrevPageId(newFreePage.GetPrevPageId())
			err = store.CommitPage(currentPage)
			if err != nil {return err}
			err = store.CommitPage(newFreePage)
			if err != nil {return err}
			return nil
		}
	}
	panic("logic error: page extension loop exit without finding a page")
}

func writeIndexToStore(indexSnap *structure.IndexPage, currentPage *structure.BasicPage, store *PageStorage) error {
	rdr := indexSnap.Freeze()
	err := currentPage.Write(rdr, 0, rdr.Len())
	if err != nil {
		return err
	}
	err = store.CommitPage(currentPage)
	if err != nil {
		return err
	}
	store.Flush()
	return nil
}

// getPagePhysicalLocation calculates the byte offset of the start of a page
func getPagePhysicalLocation(pageId int) int64{
	return int64(PSHeaderSize)+(int64(pageId)*int64(structure.PageRawSize))
}

// allocatePageBlock reserves a set of new pages for use, and returns their IDs.
// This may allocate new pages and/or reuse released pages.
func (store *PageStorage)allocatePageBlock(pageIds []int) error{
	if len(pageIds) < 1 {
		return nil
	}

	//store.lock.Lock()
	//defer store.lock.Unlock()

	stopId, err := store.reassignReleasedPages(pageIds)
	if err != nil {return err}

	store.directlyAllocatePages(pageIds, stopId)

	return nil
}

// writeStreamInternal writes a stream to a known set of page IDs
func (store *PageStorage) writeStreamInternal(data []byte, pageIds []int, pagesRequired int) (endPageId int, err error) {
	prev := -1
	var page *structure.BasicPage
	for i := 0; i < pagesRequired; i++ {
		page, err = store.GetRawPage(pageIds[i])
		if err != nil {return 0, err}
		if page == nil {return 0, errors.New("failed to load page")}

		err = page.Write(bytes.NewBuffer(data), 0, structure.PageDataCapacity)
		if err != nil {return 0, err}
		page.SetPrevPageId(int32(prev))

		err = store.CommitPage(page)
		if err != nil {return 0, err}
		prev = page.PageId
	}
	return prev, nil
}

// reassignReleasedPages recovers pages from the free list.
// Returns the last index that couldn't be filled (array length if everything was filled).
func (store *PageStorage) reassignReleasedPages(block []int) (int, error) {
	var i int
	for i = 0; i < len(block); i++ {
		pageId, err := store.reassignSinglePage()
		if err != nil {return i, err}
		if pageId < 0 {return i, nil}

		block[i] = int(pageId)
	}
	return i, nil
}

func (store *PageStorage)reassignSinglePage() (pageId int32, err error){
	freeLink := store.GetFreeListLink()
	hasList, topPageId := freeLink.TryGetLink(0)
	if !hasList {return badPage, nil} // no pages ever freed
	if topPageId < 0 {return badPage, nil} // no free pages

	topPage,err := store.GetRawPage(topPageId)
	if err != nil {return badPage, err}
	if topPage == nil {return badPage, errors.New("invalid free page link")}

	// The plan:
	// If top free-list page is not empty, use a slot
	// Else use the free-list page itself, and move the free-list pointer down one.

	// Structure of free pages' data (see also `releaseSinglePage`)
	// [Entry count: int32] -> n
	// n * [PageId: int32]

	length, err := topPage.ReadDataInt32(0)
	if err != nil {return badPage, err}
	if length > structure.MaxInt32Index {
		panic(length)
	}
	if length > 0 {
		// we have free pages. Assign one, and commit changes back
		pageId, err = topPage.ReadDataInt32(int(length))
		if err != nil {return badPage, err}

		// blank out this slot and reduce the length
		if err = topPage.WriteDataInt32(int(length), 0); err != nil {return badPage, err}
		if err = topPage.WriteDataInt32(0, length - 1); err != nil {return badPage, err}

		err = store.CommitPage(topPage)
		if err != nil {return badPage, err}

		return pageId, nil
	}
	// otherwise, we have an empty free page.
	// if we're at the head of the free-list, we can't do anything much
	nextFree := topPage.GetPrevPageId()
	if nextFree < 0 {return badPage, nil}

	// TODO: is this true? Don't we keep expired pages?

	// unlink the page
	//exp := freeLink.WriteNewLink(int(topPage.GetPrevPageId()))
	//if exp != topPageId {panic(fmt.Sprintf("Expected to expire %v, but got %v", topPageId, exp))}
	//store.SetFreeListLink(freeLink)

	return badPage, nil
}

func reverseInPlace(list []int) {
	end := len(list) - 1
	for i := 0; i < end; i++ {
		if i >= end {return}
		list[i], list[end] = list[end], list[i]
		end--
	}
}

// directlyAllocatePages writes new pages to the storage stream, without checking the free page list.
// This is used internally only, and we should always have a lock before calling.
func (store *PageStorage) directlyAllocatePages(ids []int, startIdx int) {
	for i := startIdx; i < len(ids); i++ {
		baseLength, err := store.file.Seek(0, io.SeekEnd)
		if err != nil {panic(err)}
		pageId := int((1 + baseLength - PSHeaderSize) / structure.PageRawSize)
		ids[i] = pageId
		err = store.CommitPage(structure.NewBasicPage(pageId))
		if err != nil {panic(err)}
	}
}

// setLink updates the version link for a special page. Acquire a lock before calling.
func (store *PageStorage) setLink(i int, value *support.VersionedLink){
	if value == nil {panic("tried to set a core version to an invalid value")}

	stream := value.Freeze()
	_, err := store.file.Seek(calcLinkOffset(i), io.SeekStart)
	if err != nil {panic(err)}

	_, err = io.Copy(store.file, stream)
	if err != nil {panic(err)}
}

// getLink finds the version link for a special page. Acquire a lock before calling.
func (store *PageStorage) getLink(i int) *support.VersionedLink {
	result := support.NewVersionedLink()

	_, err := store.file.Seek(calcLinkOffset(i), io.SeekStart)
	if err != nil {panic(err)}

	err = result.Defrost(store.file)
	if err != nil {panic(err)}

	return result
}

func calcLinkOffset(headOffset int) int64{
	return int64(PSMagicSize) + (int64(support.VersionLinkByteSize) * int64(headOffset))
}
