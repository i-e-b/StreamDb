using System;
using System.Collections.Generic;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.DbStructure;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.Core
{
    /// <summary>
    /// A simplified version of the original PageTable. This one uses only reverse links and a streaming interface.
    /// The start of this database is not a full page, but a set of versioned links out to:
    ///  - an index page chain
    ///  - a path-lookup page chain
    ///  - a free-list page chain
    /// All page version links point to the end of a chain.
    /// Pages cannot be updated in this store -- write a new copy and release the old one.
    /// <para></para>
    /// Unlike the PageTable, this handles its free page list directly and internally. The main index and path lookup are normal documents with no special position.
    /// </summary>
    public class PageStreamStorage {
        [NotNull] private readonly Stream _fs;
        [NotNull] private readonly object fslock = new object();

        /// <summary> A magic number we use to recognise our database format </summary>
        [NotNull] public static readonly byte[] HEADER_MAGIC = { 0x55, 0xAA, 0xFE, 0xED, 0xFA, 0xCE, 0xDA, 0x7A };

        public const int MAGIC_SIZE = 8;
        public const int HEADER_SIZE = (VersionedLink.ByteSize * 3) + MAGIC_SIZE;
        public const int FREE_PAGE_SLOTS = 128;

        public PageStreamStorage([NotNull]Stream fs)
        {
            _fs = fs;
            if (!fs.CanRead) throw new Exception("Database stream must be readable");
            if (!fs.CanSeek) throw new Exception("Database stream must support seeking");

            // Create empty database?
            if (fs.Length == 0) {
                InitialiseDB(fs);
                return;
            }

            if (fs.Length < HEADER_SIZE) throw new Exception("Stream is not empty, but is to short to read header information");

            // Not empty -- quick sanity check that our stream is a real DB
            fs.Seek(0, SeekOrigin.Begin);
            foreach (var b in HEADER_MAGIC)
            {
                if (fs.ReadByte() != b) throw new Exception("Supplied stream is not a StreamDB file");
            }
        }

        public static void InitialiseDB([NotNull]Stream fs)
        {
            if (!fs.CanWrite) throw new Exception("Tried to initialise a read-only stream");

            fs.Seek(0, SeekOrigin.Begin);
            foreach (var b in HEADER_MAGIC) { fs.WriteByte(b); }

            // write disabled links for the three core chains
            var indexVersion = new VersionedLink();
            var pathLookupVersion = new VersionedLink();
            var freeListVersion = new VersionedLink();

            indexVersion.Freeze().CopyTo(fs);
            pathLookupVersion.Freeze().CopyTo(fs);
            freeListVersion.Freeze().CopyTo(fs);
            fs.Flush();
        }

        /// <summary>
        /// Get a read-only page stream for a page chain, given it's end ID
        /// </summary>
        public SimplePageStream GetStream(int endPageId) {
            return new SimplePageStream(this, endPageId);
        }

        /// <summary>
        /// Write a data stream from its current position to end to a new page chain. Returns the end page ID.
        /// This ID should then be stored either inside the index document, or to one of the core versions.
        /// </summary>
        public int WriteStream(Stream dataStream) {
            if (dataStream == null) throw new Exception("Data stream must be valid");

            var bytesRequired = dataStream.Length - dataStream.Position;
            var pagesRequired = SimplePage.CountRequired(bytesRequired);

            var pages = new int[pagesRequired];
            AllocatePageBlock(pages);

            return WriteStreamInternal(dataStream, pagesRequired, pages);
        }

        /// <summary>
        /// Reserve a set of new pages for use, and return their IDs.
        /// This may allocate new pages and/or reuse released pages.
        /// </summary>
        /// <param name="block">Array for pages required. All slots will be filled with new page IDs</param>
        public void AllocatePageBlock(int[] block)
        {
            if (block == null) throw new Exception("Requested free pages for a null block");
            if (block.Length < 1) return;

            lock (fslock) {
                // Exhaust the free page list to fill our block.
                // If we run out of free pages, allocate the rest at the end of the stream
                var stopIdx = ReassignReleasedPages(block);
                DirectlyAllocatePages(block, stopIdx);
            }
        }

        /// <summary>
        /// Release all pages in a chain. They can be reused on next write.
        /// If the page ID given is invalid, the release command is silently ignored
        /// </summary>
        public void ReleaseChain(int endPageId) {
            if (endPageId < 0) return;

            var pagesSeen = new HashSet<int>();
            var currentPage = GetRawPage(endPageId);
            // walk down the chain
            while (currentPage != null)
            {
                if (pagesSeen.Contains(currentPage.PageId)) throw new Exception($"Loop in chain {endPageId} at ID = {currentPage.PageId}");
                pagesSeen.Add(currentPage.PageId);

                ReleaseSinglePage(currentPage.PageId);
                currentPage = GetRawPage(currentPage.PrevPageId);
            }
        }

        /// <summary>
        /// Read a page from the storage stream to memory. This will check the CRC.
        /// </summary>
        [CanBeNull]public SimplePage GetRawPage(int pageId)
        {
            if (pageId < 0) return null;
            lock (fslock)
            {
                _fs.Seek(HEADER_SIZE + (pageId * SimplePage.PageRawSize), SeekOrigin.Begin);
                var result = new SimplePage(pageId);
                result.Defrost(_fs);
                if (!result.ValidateCrc()) throw new Exception($"Reading page {pageId} failed CRC check");
                return result;
            }
        }

        /// <summary>
        /// Write a page from memory to storage. This will update the CRC before writing.
        /// </summary>
        public void CommitPage(SimplePage page) {
            if (page == null) throw new Exception("Can't commit a null page");
            if (page.PageId < 0) throw new Exception("Page ID must be valid");

            var pageId = page.PageId;
            page.UpdateCRC();

            var ms = new MemoryStream(SimplePage.PageRawSize);
            page.Freeze().CopyTo(ms);
            ms.Seek(0, SeekOrigin.Begin);
            var buffer = ms.ToArray() ?? throw new Exception($"Failed to serialise page {pageId}");

            lock (fslock)
            {
                _fs.Seek(HEADER_SIZE + (pageId * SimplePage.PageRawSize), SeekOrigin.Begin);
                _fs.Write(buffer, 0, buffer.Length);
                _fs.Flush();
            }
        }
        
        /// <summary>
        /// Map a document GUID to a page ID.
        /// If the document has an existing page, the versions will be incremented.
        /// If a version expires, the page ID will be returned in `expiredPageId`
        /// </summary>
        /// <param name="documentId">Unique ID for the document</param>
        /// <param name="newPageId">top page id for most recent version of the document</param>
        /// <param name="expiredPageId">an expired version of the document, or `-1` if no versions have expired</param>
        public void BindIndex(Guid documentId, int newPageId, out int expiredPageId)
        {
            lock (fslock)
            {
                var indexLink = GetIndexPageLink();
                if (!indexLink.TryGetLink(0, out var indexTopPageId))
                {
                    indexTopPageId = -1;
                }

                // Try to update an existing document
                var currentPage = GetRawPage(indexTopPageId);
                while (currentPage != null)
                {
                    var indexSnap = new IndexPage();
                    indexSnap.Defrost(currentPage.BodyStream());

                    var found = indexSnap.Update(documentId, newPageId, out expiredPageId);
                    if (found)
                    {
                        var stream = indexSnap.Freeze();
                        currentPage.Write(stream, 0, stream.Length);
                        CommitPage(currentPage);
                        return;
                    }

                    currentPage = GetRawPage(currentPage.PrevPageId);
                }

                // Try to insert a new link in an existing index page
                expiredPageId = -1;
                currentPage = GetRawPage(indexTopPageId);
                while (currentPage != null)
                {
                    var indexSnap = new IndexPage();
                    indexSnap.Defrost(currentPage.BodyStream());

                    var found = indexSnap.TryInsert(documentId, newPageId);
                    if (found)
                    {
                        var stream = indexSnap.Freeze();
                        currentPage.Write(stream, 0, stream.Length);
                        CommitPage(currentPage);
                        return;
                    }

                    currentPage = GetRawPage(currentPage.PrevPageId);
                }

                // need to extend into a new index, and write to a new version of the head
                var newIndex = new IndexPage();
                var ok = newIndex.TryInsert(documentId, newPageId);
                if (!ok) throw new Exception("Failed to write index to blank index page");
                var slot = new int[1];
                AllocatePageBlock(slot);
                var newPage = GetRawPage(slot[0]) ?? throw new Exception("Failed to read newly allocated page");
                newPage.PrevPageId = indexTopPageId;
                var newStream = newIndex.Freeze();
                newPage.Write(newStream, 0, newStream.Length);
                CommitPage(newPage);

                // set new head link
                indexLink.WriteNewLink(newPage.PageId, out _); // Index is always extended, we never clean it up
                SetIndexPageLink(indexLink);
                _fs.Flush();
            }
        }

        /// <summary>
        /// Remove a mapping from a document GUID.
        /// The page chain is not affected.
        /// If no such document id is bound, nothing happens
        /// </summary>
        public void UnbindIndex(Guid documentId)
        {
            lock (fslock)
            {
                var indexLink = GetIndexPageLink();
                if (!indexLink.TryGetLink(0, out var indexTopPageId)) {
                     return; // no index to unbind
                }

                // Search for the binding, and remove if found
                var currentPage = GetRawPage(indexTopPageId);
                while (currentPage != null)
                {
                    var indexSnap = new IndexPage();
                    indexSnap.Defrost(currentPage.BodyStream());

                    var found = indexSnap.Remove(documentId);
                    if (found)
                    {
                        var stream = indexSnap.Freeze();
                        currentPage.Write(stream, 0, stream.Length);
                        CommitPage(currentPage);
                        _fs.Flush();
                        return;
                    }

                    currentPage = GetRawPage(currentPage.PrevPageId);
                }
            }
        }

        /// <summary>
        /// Get the top page ID for a document ID by reading the index.
        /// If the document ID can't be found, returns -1
        /// </summary>
        public int GetDocumentHead(Guid documentId)
        {
            var indexLink = GetIndexPageLink();
            if (!indexLink.TryGetLink(0, out var indexTopPageId))
            {
                indexTopPageId = -1;
            }

            // Try to update an existing document
            var currentPage = GetRawPage(indexTopPageId);
            while (currentPage != null)
            {
                var indexSnap = new IndexPage();
                indexSnap.Defrost(currentPage.BodyStream());

                var found = indexSnap.Search(documentId, out var link);
                if (found)
                {
                    if (link.TryGetLink(0, out var result)) return result;
                }

                currentPage = GetRawPage(currentPage.PrevPageId);
            }
            return -1;
        }

        /// <summary>
        /// Bind an exact path to a document ID.
        /// If an existing document was bound to the same path, its ID will be returned
        /// </summary>
        /// <param name="path">Exact path for document</param>
        /// <param name="documentId">new document id</param>
        /// <param name="previousDocId">old document id that has been replaced, if any.</param>
        public void BindPath(string path, Guid documentId, out Guid? previousDocId)
        {
            previousDocId = null;
            if (string.IsNullOrEmpty(path)) throw new Exception("Path must not be null or empty");

            lock (fslock)
            {
                // Read current path document (if it exists)
                var pathLink = GetPathLookupLink();
                var pathIndex = new ReverseTrie<SerialGuid>();
                if (pathLink.TryGetLink(0, out var pathPageId))
                {
                    pathIndex.Defrost(GetStream(pathPageId));
                }

                // Bind the path
                var sguid = pathIndex.Add(path, documentId);
                if (sguid != null) previousDocId = sguid.Value;

                // Write back to new chain
                var newPageId = WriteStream(pathIndex.Freeze());

                // Update version link
                pathLink.WriteNewLink(newPageId, out var expired);
                SetPathLookupLink(pathLink);

                ReleaseChain(expired);
                _fs.Flush();
            }
        }

        /// <summary>
        /// Read the path lookup, and return the DocumentID stored at the exact path.
        /// Returns null if there is not document stored.
        /// </summary>
        public Guid? GetDocumentIdByPath(string exactPath)
        {
            var pathLink = GetPathLookupLink();
            var pathIndex = new ReverseTrie<SerialGuid>();
            if (!pathLink.TryGetLink(0, out var pathPageId)) return null;
            pathIndex.Defrost(GetStream(pathPageId));

            var found = pathIndex.Get(exactPath);
            if (found == null) return null;
            return found.Value;
        }

        /// <summary>
        /// Return all paths currently bound for the given document ID.
        /// If no paths are bound, an empty enumeration is given.
        /// </summary>
        [NotNull]public IEnumerable<string> GetPathsForDocument(Guid documentId)
        {
            var pathLink = GetPathLookupLink();
            var pathIndex = new ReverseTrie<SerialGuid>();
            if (!pathLink.TryGetLink(0, out var pathPageId)) return new string[0];
            pathIndex.Defrost(GetStream(pathPageId));

            return pathIndex.GetPathsForEntry(documentId);
        }

        /// <summary>
        /// Return all paths currently bound that start with the given prefix.
        /// The prefix should not be null or empty.
        /// If no paths are bound, an empty enumeration is given.
        /// </summary>
        [NotNull]public IEnumerable<string> SearchPaths(string pathPrefix)
        {
            var pathLink = GetPathLookupLink();
            var pathIndex = new ReverseTrie<SerialGuid>();
            if (!pathLink.TryGetLink(0, out var pathPageId)) return new string[0];
            pathIndex.Defrost(GetStream(pathPageId));

            return pathIndex.Search(pathPrefix);
        }

        /// <summary>
        /// Remove a path binding if it exists. If the path is not bound, nothing happens.
        /// Linked documents are not removed.
        /// </summary>
        public void UnbindPath(string exactPath)
        {
            lock (fslock)
            {
                var pathLink = GetPathLookupLink();
                var pathIndex = new ReverseTrie<SerialGuid>();
                if (!pathLink.TryGetLink(0, out var pathPageId)) return;
                pathIndex.Defrost(GetStream(pathPageId));

                // Unbind the path
                pathIndex.Delete(exactPath);

                // Write back to new chain
                var newPageId = WriteStream(pathIndex.Freeze());

                // Update version link
                pathLink.WriteNewLink(newPageId, out var expired);
                SetPathLookupLink(pathLink);

                ReleaseChain(expired);
                _fs.Flush();
            }
        }



        /// <summary>
        /// Write a stream to a known set of page IDs
        /// </summary>
        private int WriteStreamInternal([NotNull]Stream dataStream, int pagesRequired, [NotNull]int[] pages)
        {
            var prev = -1;
            for (int i = 0; i < pagesRequired; i++)
            {
                var page = GetRawPage(pages[i]);
                if (page == null) throw new Exception($"Failed to load page {pages[i]}");
                page.Write(dataStream, 0, SimplePage.PageDataCapacity);
                page.PrevPageId = prev;

                CommitPage(page);
                prev = page.PageId;
            }

            return prev;
        }

        /// <summary>
        /// Allocate pages to a block without checking the free page list
        /// </summary>
        private void DirectlyAllocatePages([NotNull]int[] block, int startIdx)
        {
            for (int i = startIdx; i < block.Length; i++)
            {
                var pageId = (int) ((1 + _fs.Length - HEADER_SIZE) / SimplePage.PageRawSize);
                block[i] = pageId;
                CommitPage(new SimplePage(block[i]));
            }
        }

        /// <summary>
        /// Recover pages from the free list. Returns the last index that couldn't be filled (array length if everything was filled)
        /// </summary>
        private int ReassignReleasedPages([NotNull]int[] block)
        {
            var hasList = GetFreeListLink().TryGetLink(0, out var topPageId);
            if (!hasList) return 0;

            var topPage = GetRawPage(topPageId);
            if (topPage == null) return 0;

            // Structure of free pages' data (see also `ReleaseSinglePage`)
            // [Entry count: int32] -> n
            // n * [PageId: int32]

            // The plan:
            // - walk back through the chain
            // - if we hit an empty end page that is not the top page, use that as the free page, and tidy up the back link. Go up a page if possible
            // - if we're on a non empty end page, use the entries and clear them
            // - if we're on an empty top page, give up and return our position

            var linkStack = new Stack<int>();
            var currentPage = topPage;
            // walk down the chain
            while (currentPage.PrevPageId >= 0) {
                linkStack.Push(currentPage.PageId);
                currentPage = GetRawPage(currentPage.PrevPageId) ?? throw new Exception("Free page chain is broken.");
            }

            int i;
            for (i = 0; i < block.Length; i++) // each required page
            {
                // check if free list page is non-empty
                var length = currentPage.ReadDataInt32(0);
                if (length < 1) // page is empty
                {
                    if (currentPage.PageId == topPageId) return i; // ran out of free data

                    block[i] = currentPage.PageId; // use this empty page
                    currentPage = GetRawPage(linkStack.Pop()) ?? throw new Exception("Free page walk up lost");
                    currentPage.PrevPageId = -1; // break link to the recovered page
                    CommitPage(currentPage);
                }
                else // page has free links remaining
                {
                    block[i] = currentPage.ReadDataInt32(length); // copy id
                    currentPage.WriteDataInt32(0, length - 1); // remove from stack
                    CommitPage(currentPage); // save changes
                }
            }

            return i;
        }
        
        /// <summary>
        /// Add a single page to release chain.
        /// This will create free list pages as required
        /// </summary>
        private void ReleaseSinglePage(int pageToReleaseId)
        {
            // Note: if we need to extend the free list, we should use the last page in the current list.
            // So, we can't assume pages are full based on prevPageId value.
            lock (fslock)
            {
                var freeLink = GetFreeListLink();
                var hasList = freeLink.TryGetLink(0, out var topPageId);
                if (!hasList) {
                    // need to create a new page and set it up
                    var slot = new int[1];
                    DirectlyAllocatePages(slot, 0);
                    freeLink.WriteNewLink(slot[0], out _);
                    topPageId = slot[0];
                    SetFreeListLink(freeLink);
                    _fs.Flush();
                }

                // Structure of free pages' data (see also `ReassignReleasedPages`)
                // [Entry count: int32] -> n
                // n * [PageId: int32]

                var currentPage = GetRawPage(topPageId) ?? throw new Exception($"Lost free list page (id = {topPageId})");
                while (currentPage != null)
                {
                    // check if there's space on this page
                    var length = currentPage.ReadDataInt32(0);

                    if (length < SimplePage.MaxInt32Index) // Space remains. Write value and exit
                    {
                        length++;
                        currentPage.WriteDataInt32(length, pageToReleaseId);
                        currentPage.WriteDataInt32(0, length);
                        CommitPage(currentPage);
                        return;
                    }

                    // walk page chain
                    if (currentPage.PrevPageId >= 0) {
                        currentPage = GetRawPage(currentPage.PrevPageId);
                    } else {
                        // use the new free page to extend the list.
                        var newFreePage = GetRawPage(pageToReleaseId) ?? throw new Exception($"Failed to read released page {pageToReleaseId}");
                        newFreePage.ZeroAllData();
                        newFreePage.PrevPageId = -1;
                        CommitPage(newFreePage);
                        currentPage.PrevPageId = newFreePage.PageId;
                        CommitPage(currentPage);
                        return;
                    }
                }

                throw new Exception("Page extension failed");
            }
        }

        [NotNull]private VersionedLink GetIndexPageLink() { return GetLink(0); }
        private void SetIndexPageLink(VersionedLink value) { SetLink(0, value); }
        
        [NotNull]private VersionedLink GetPathLookupLink() { return GetLink(1); }
        private void SetPathLookupLink(VersionedLink value) { SetLink(1, value); }

        [NotNull]private VersionedLink GetFreeListLink() { return GetLink(2); }
        private void SetFreeListLink(VersionedLink value) { SetLink(2, value); }

        private void SetLink(int headOffset, VersionedLink value)
        {
            if (value == null) throw new Exception("Attempted to set invalid header link");
            lock (fslock)
            {
                _fs.Seek(MAGIC_SIZE + (VersionedLink.ByteSize * headOffset), SeekOrigin.Begin);
                value.Freeze().CopyTo(_fs);
            }
        }

        [NotNull]private VersionedLink GetLink(int headOffset)
        {
            lock (fslock)
            {
                var result = new VersionedLink();
                _fs.Seek(MAGIC_SIZE + (VersionedLink.ByteSize * headOffset), SeekOrigin.Begin);
                result.Defrost(_fs);
                return result;
            }
        }

    }
}