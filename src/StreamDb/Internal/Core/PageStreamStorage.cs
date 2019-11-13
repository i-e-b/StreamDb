using System;
using System.Collections.Generic;
using System.IO;
using JetBrains.Annotations;
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
                prev = page.OriginalPageId;
            }

            return prev;
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

            Console.WriteLine($"Request for {block.Length} new pages");
            lock (fslock) {
                // Exhaust the free page list to fill our block.
                // If we run out of free pages, allocate the rest at the end of the stream
                var stopIdx = ReassignReleasedPages(block);
                DirectlyAllocatePages(block, stopIdx);
            }
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
                linkStack.Push(currentPage.OriginalPageId);
                currentPage = GetRawPage(currentPage.PrevPageId) ?? throw new Exception("Free page chain is broken.");
            }

            int i;
            for (i = 0; i < block.Length; i++) // each required page
            {
                // check if free list page is non-empty
                var length = currentPage.ReadDataInt32(0);
                if (length < 1) // page is empty
                {
                    if (currentPage.OriginalPageId == topPageId) return i; // ran out of free data

                    block[i] = currentPage.OriginalPageId; // use this empty page
                    Console.WriteLine($"Reassigned page from free chain {block[i]}");
                    currentPage = GetRawPage(linkStack.Pop()) ?? throw new Exception("Free page walk up lost");
                    currentPage.PrevPageId = -1; // break link to the recovered page
                    CommitPage(currentPage);
                }
                else // page has free links remaining
                {
                    block[i] = currentPage.ReadDataInt32(length); // copy id
                    Console.WriteLine($"Reassigned page {block[i]}");
                    currentPage.WriteDataInt32(0, length - 1); // remove from stack
                    CommitPage(currentPage); // save changes
                }
            }

            return i;
        }


        /// <summary>
        /// Release all pages in a chain. They can be reused on next write.
        /// If the page ID given is invalid, the release command is silently ignored
        /// </summary>
        public void ReleaseChain(int endPageId) {
            Console.WriteLine($"Request to release {endPageId}");
            if (endPageId < 0) return;

            var pagesSeen = new HashSet<int>();
            var currentPage = GetRawPage(endPageId);
            // walk down the chain
            while (currentPage != null)
            {
                if (pagesSeen.Contains(currentPage.OriginalPageId)) throw new Exception($"Loop in chain {endPageId} at ID = {currentPage.OriginalPageId}");
                pagesSeen.Add(currentPage.OriginalPageId);

                ReleaseSinglePage(currentPage.OriginalPageId);
                currentPage = GetRawPage(currentPage.PrevPageId);
            }
        }

        /// <summary>
        /// Add a single page to release chain.
        /// This will create free list pages as required
        /// </summary>
        private void ReleaseSinglePage(int pageId)
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
                // check if there's space on this page
                var length = currentPage.ReadDataInt32(0);

                if (length < SimplePage.MaxInt32Index) // Space remains. Write value and exit
                {
                    length++;
                    currentPage.WriteDataInt32(length, pageId);
                    currentPage.WriteDataInt32(0, length);
                    CommitPage(currentPage);
                    return;
                }

                throw new Exception("Page extension not yet implemented");
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
            if (page.OriginalPageId < 0) throw new Exception("Page ID must be valid");

            var pageId = page.OriginalPageId;
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
        
        

        [NotNull]private VersionedLink GetIndexPageLink() { return GetLink(0); }
        private void SetIndexPageLink(VersionedLink value) { SetLink(0, value); }
        
        [NotNull]private VersionedLink GetPathLookupLink() { return GetLink(1); }
        private void SetPathLookupLink(VersionedLink value) { SetLink(1, value); }

        [NotNull]private VersionedLink GetFreeListLink() { return GetLink(2); }
        private void SetFreeListLink(VersionedLink value) { SetLink(2, value); }

        private void SetLink(int headOffset, VersionedLink value)
        {
            if (value == null) throw new Exception("Attempted to set invalid header link");
            lock(fslock) {
                _fs.Seek(MAGIC_SIZE + (VersionedLink.ByteSize * headOffset), SeekOrigin.Begin);
                value.Freeze().CopyTo(_fs);
            }
        }

        [NotNull]private VersionedLink GetLink(int headOffset)
        {
            lock(fslock) {
                var result = new VersionedLink();
                _fs.Seek(MAGIC_SIZE + (VersionedLink.ByteSize * headOffset), SeekOrigin.Begin);
                result.Defrost(_fs);
                return result;
            }
        }

    }
}