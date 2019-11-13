using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using JetBrains.Annotations;
using StreamDb.Internal.DbStructure;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.Core
{
    /// <summary>
    /// Holds crap that should be removed
    /// </summary>
    public class PageTable : PageTableCore, IDatabaseBackend
    {
        /// <inheritdoc />
        public PageTable([NotNull] Stream fs) : base(fs) { 
            // Basic sanity tests
            StartupSanityTests(fs);
            
            // Check the structure is ready
            var root = ReadRoot();
            if (!root.FreeListLink.TryGetLink(0, out _)) throw new Exception("Database free page list is damaged");
            if (!root.IndexLink.TryGetLink(0, out _)) throw new Exception("Database index table is damaged");
            if (!root.PathLookupLink.TryGetLink(0, out _)) throw new Exception("Database path lookup table is damaged");
        }


        private void StartupSanityTests([NotNull]Stream fs)
        {
            if (fs.Length == 0) // Empty. Initialise with a root page
            {
                InitialisePageTable();
            }
            else if (fs.Length < (ComplexPage.PageRawSize*3)) // can't have a valid structure
            {
                if (fs.Length >= 8)
                {
                    // has some data
                    var reader = _storage.AcquireReader();
                    try
                    {
                        var magic = reader.ReadUInt64();
                        if (magic != HEADER_MAGIC) throw new Exception("This is a non-empty stream that is not a valid StreamDb");
                    }
                    finally
                    {
                        _storage.Release(ref reader);
                    }
                }

                throw new Exception("This database is too heavily truncated to recover");
            }
        }

        private void InitialisePageTable()
        {
            // NOTE:
            //      This is NOT the normal way of setting up pages.
            //      This is ONLY for the initial setup of a blank DB.
            //

            var index0 = new IndexPage();
            var free0 = new FreeListPage();
            var path0 = new ReverseTrie<SerialGuid>();

            // we use fixed positions for the default pages. Set initial versions
            var root0 = new RootPage();
            root0.AddIndex(pageId: 1, out _);
            root0.AddFreeList(pageId: 2, out _);
            root0.AddPathLookup(pageId: 3, out _);

            var root = NewRootPage(root0);
            var index = NewIndexForBlankDB(index0);
            var free = NewFreeListForBlankDB(free0);
            var path = NewPathLookupForBlankDB(path0);

            // Write to storage
            var w = _storage.AcquireWriter();
            try {
                w.Seek(0, SeekOrigin.Begin);

                root.Freeze().CopyTo(w.BaseStream);
                index.Freeze().CopyTo(w.BaseStream);
                free.Freeze().CopyTo(w.BaseStream);
                path.Freeze().CopyTo(w.BaseStream);
                w.Flush();
            }
            finally 
            {
                _storage.Release(ref w);
            }

        }
        
        [NotNull]private static ComplexPage NewPathLookupForBlankDB([NotNull]ReverseTrie<SerialGuid> path0)
        {
            var page = new ComplexPage
            {
                PageType = PageType.PathLookup,
                NextPageId = ComplexPage.NextIdForEmptyPage,
                PrevPageId = -1,
                DocumentSequence = 0,
                DocumentId = ComplexPage.PathLookupGuid,
                Dirty = true,
                FirstPageId = 3
            };
            var bytes = path0.Freeze();
            
            page.Write(bytes, 0, 0, bytes.Length);
            page.UpdateCRC();
            return page;
        }
        
        [NotNull]private static ComplexPage NewFreeListForBlankDB([NotNull]FreeListPage free0)
        {
            var page = new ComplexPage
            {
                PageType = PageType.ExpiredList,
                NextPageId = ComplexPage.NextIdForEmptyPage,
                PrevPageId = -1,
                DocumentSequence = 0,
                DocumentId = ComplexPage.FreePageGuid,
                Dirty = true,
                FirstPageId = 2
            };
            var bytes = free0.Freeze();
            page.Write(bytes, 0, 0, bytes.Length);
            page.UpdateCRC();
            return page;
        }

        [NotNull]private static ComplexPage NewIndexForBlankDB([NotNull]IndexPage index0)
        {
            var index = new ComplexPage
            {
                PageType = PageType.Index,
                NextPageId = ComplexPage.NextIdForEmptyPage,
                PrevPageId = -1,
                DocumentSequence = 0,
                DocumentId = ComplexPage.IndexTreeGuid,
                Dirty = true,
                FirstPageId = 1
            };
            var indexBytes = index0.Freeze();
            index.Write(indexBytes, 0, 0, indexBytes.Length);
            index.UpdateCRC();
            return index;
        }

        [NotNull]private static ComplexPage NewRootPage([NotNull]RootPage root0)
        {
            var root = new ComplexPage
            {
                PageType = PageType.Root,
                NextPageId = ComplexPage.NextIdForEmptyPage,
                PrevPageId = -1,
                DocumentSequence = 0,
                DocumentId = ComplexPage.RootDocumentGuid,
                Dirty = true,
                FirstPageId = 0
            };
            var rootBytes = root0.Freeze();
            root.Write(rootBytes, 0, 0, rootBytes.Length);
            root.UpdateCRC();
            return root;
        }

    }



    /// <summary>
    /// The page table class helps manage all the little bits of the database, but it does not present a user-interface.
    /// Always use the `Database` class unless you are doing very low level work.
    ///
    /// This is the core of the database format.
    /// Our database starts with a root page that links to:
    ///  - an index page chain
    ///  - a path lookup chain
    ///  - a free page chain
    /// Occupied document data pages are not listed in the root, these are reachable from the index pages.
    /// A valid page table never has less than 4 pages (pageID 0..3) -- so pageID 4 is the first valid page to allocate
    /// </summary>
    /// <remarks>
    /// This class is turning into a bit of a mess. TODO: rebuild this with better abstractions once the DB is functioning.
    /// </remarks>
    public class PageTableCore
    {
        /// <summary> A magic number we use to recognise our database format </summary>
        public const ulong HEADER_MAGIC = 0x572E_FEED_FACE_DA7A;

        /// <summary>
        /// Maximum page index we support
        /// </summary>
        const long MAXPAGE = int.MaxValue / ComplexPage.PageRawSize;


        /*
         * Page table should have a single root entry that is not updated unless absolutely needed (helps with thread safety)
         * This should have links to:
         *   - a GUID tree (for document ID -> Page lookups); Guid tree nodes should have a list of versions, each to a chain of data
         *   - Data page list
         * maybe more, but see if we can cope with just this
         */
        [NotNull]protected readonly InterlockBinaryStream _storage;
        
        /// <summary>
        /// A lock around getting a new page
        /// </summary>
        [NotNull]private readonly object _newPageLock = new object();

        [CanBeNull] private RootPage _rootPageCache = null;
        [CanBeNull] private ReverseTrie<SerialGuid> _pathIndexCache = null;

        public PageTableCore([NotNull]Stream fs)
        {
            fs.Seek(0, SeekOrigin.Begin);
            _storage = new InterlockBinaryStream(fs, false);
        }

        /// <summary>
        /// Read a specific existing page by Page ID. Returns null if the page does not exist.
        /// </summary>
        [CanBeNull]public Page<T> GetPageView<T>(int pageId) where T : IStreamSerialisable, new()
        {
            var reader = _storage.AcquireReader();
            try {
                if (reader.BaseStream == null) throw new Exception("Page table base stream is invalid");
                if (pageId < 0) return null;

                var byteOffset = pageId * ComplexPage.PageRawSize;
                var byteEnd = byteOffset + ComplexPage.PageRawSize;

                if (reader.BaseStream.Length < byteEnd) return null;

                reader.BaseStream.Seek(byteOffset, SeekOrigin.Begin);
                return new Page<T>(pageId, new Substream(reader.BaseStream, ComplexPage.PageRawSize));
            } finally {
                _storage.Release(ref reader);
            }
        }

        /// <summary>
        /// Read a raw page block (without a view)
        /// </summary>
        /// <param name="pageId">Page ID</param>
        /// <param name="ignoreCrc">Optional: if true, the CRC will not be checked</param>
        [CanBeNull]public ComplexPage GetPageRaw(int pageId, bool ignoreCrc = false)
        {
            var reader = _storage.AcquireReader();
            try {
                if (reader.BaseStream == null) throw new Exception("Page table base stream is invalid");
                if (pageId < 0) return null; // this makes page walking simpler

                var byteOffset = pageId * ComplexPage.PageRawSize;
                var byteEnd = byteOffset + ComplexPage.PageRawSize;

                if (reader.BaseStream.Length < byteEnd) throw new Exception($"Database stream is truncated at page {pageId}");

                if (byteOffset < 0) throw new Exception($"Byte offset calculated returned nonsense result (offset = {byteOffset} for pageId = {pageId})");
                reader.BaseStream.Seek(byteOffset, SeekOrigin.Begin);
                
                var result = new ComplexPage();
                result.Defrost(new Substream(reader.BaseStream, ComplexPage.PageRawSize));
                result.OriginalPageId = pageId;
                if (!ignoreCrc && !result.ValidateCrc()) throw new Exception($"CRC failed at page {pageId}");
                return result;
            } finally {
                _storage.Release(ref reader);
            }
        }

        [NotNull]
        protected RootPage ReadRoot() {
            if (_rootPageCache != null) return _rootPageCache;

            var page = GetPageRaw(0);
            if (page == null) throw new Exception("PageTable.ReadRoot: Failed to read root page");
            if (!page.ValidateCrc()) throw new Exception("PageTable.ReadRoot: Root entry failed CRC check. Must recover database.");

            var final = new RootPage();
            final.Defrost(page.GetDataStream());
            _rootPageCache = final;
            return final;
        }

        [NotNull]private Page<FreeListPage> GetFreePageList() {
            var root = ReadRoot();
            var page = GetPageView<FreeListPage>(root.GetFreeListPageId());
            if (page == null) throw new Exception("PageTable.ReadRoot: Failed to free list page");
            return page;
        }
        
        [NotNull]private Page<IndexPage> GetIndexPageList() {
            var root = ReadRoot();
            var page = GetPageView<IndexPage>(root.GetIndexListId());
            if (page == null) throw new Exception("PageTable.ReadRoot: Failed to free list page");
            return page;
        }

        /// <summary>
        /// Get a free page, either by reuse or by allocating a new page.
        /// The returned page will have a correct OriginalPageID, but will need other headers reset, and a new CRC before being committed
        /// </summary>
        [NotNull]public ComplexPage GetFreePage()
        {            // try to re-use a page:
            var freeList = GetFreePageList();

            // Walk the free page list
            while (true)
            {
                var found = freeList.View.TryGetNext(out var pageId);
                if (found && pageId > 0) { 
                    if (pageId >= MAXPAGE) throw new Exception($"Free list returned a junk page (Max = {MAXPAGE}, actual = {pageId})");
                    CommitPage(freeList); // remove our page from the free list
                    var page = GetPageRaw(pageId, ignoreCrc: true);
                    if (page == null) break;

                    // Clear data
                    page.NextPageId = ComplexPage.NextIdForEmptyPage;
                    page.PrevPageId = -1;
                    page.DocumentId = ComplexPage.FreePageGuid;
                    page.DocumentSequence = 0;
                    page.PageType = PageType.Invalid;

                    return page;
                }
                if (freeList.NextPageId <= 0) break;

                // go to next free table page
                freeList = GetPageView<FreeListPage>(freeList.NextPageId);

                if (freeList == null) break;
            }

            // none available. Write a new one:
            return AllocatePage();
        }

        /// <summary>
        /// Allocate a new page (without checking for a free page)
        /// </summary>
        [NotNull]private ComplexPage AllocatePage()
        {
            lock (_newPageLock)
            {
                var w = _storage.AcquireWriter();
                try
                {
                    var stream = w.BaseStream ?? throw new Exception("Lost connection to base stream");
                    var pageCount = stream.Length / ComplexPage.PageRawSize;

                    var p = new ComplexPage
                    {
                        OriginalPageId = (int) pageCount, // very thread sensitive!
                        PageType = PageType.Invalid,
                        NextPageId = ComplexPage.NextIdForEmptyPage,
                        PrevPageId = -1,
                        FirstPageId = (int) pageCount, // should update if chaining
                        DocumentSequence = 0,
                        DocumentId = Guid.Empty
                    };
                    CommitPage(p, w);
                    return p;
                }
                finally
                {
                    _storage.Release(ref w);
                }
            }
        }


        /// <summary>
        /// Add all pages in the chain to the free list.
        /// This cannot be used to delete IDs less than 4 (core tables)
        /// <para></para>
        /// This should be used when an add or update has caused a page link to be expired
        /// </summary>
        /// <param name="pageId">ID of any page in the chain.</param>
        /// <param name="rejectIfList">If the chain contains any of these IDs, none of the pages will be removed.</param>
        public void DeletePageChain(int pageId, params int[] rejectIfList) {
            if (pageId < 3) throw new Exception("Tried to delete a core page"); // note: the page lookup table can get entirely re-written, so it's not protected
            if (pageId >= MAXPAGE) throw new Exception("Tried to delete a page out of maximum range");


            // Build list and check integrity
            var pageIdList = GetPagesInChain(rejectIfList, pageId);

            // Now we have a set of page IDs to remove. Add them to the free list.
            var freePageList = GetFreePageList();
            foreach (var pageIdToDelete in pageIdList)
            {
                while (!freePageList.View.TryAdd(pageIdToDelete)) {
                    // need to grow free list
                    CommitPage(freePageList);
                    freePageList = WalkFreeList(freePageList);
                }
                NukePage(pageIdToDelete);
            }
            CommitPage(freePageList);
        }

        private List<int> GetPagesInChain(int[] rejectIfList, int pageId)
        {
            var tagged = GetPageRaw(pageId);
            if (tagged == null) return new List<int>();

            pageId = tagged.FirstPageId;

            var pageIdList = new List<int>();
            var loopHash = new HashSet<int>();
            var current = GetPageRaw(pageId);
            while (current != null)
            {
                if (rejectIfList?.Contains(current.OriginalPageId) == true)
                {
                    return new List<int>(); // send back an empty set
                }

                pageIdList.Add(current.OriginalPageId);
                loopHash.Add(current.OriginalPageId);
                var nextPageId = current.NextPageId;
                if (loopHash.Contains(nextPageId)) throw new Exception("Loop detected in page list.");
                current = GetPageRaw(nextPageId);
            }

            return pageIdList;
        }

        /// <summary>
        /// Write to the page to mark it deleted (this protects us from using after free)
        /// </summary>
        private void NukePage(int targetId)
        {
            if (targetId < 3) return;
            var target = GetPageRaw(targetId);
            if (target == null || target.OriginalPageId < 3) return;

            target.NextPageId = -1;
            target.DocumentId = ComplexPage.FreePageGuid;
            target.PageType = PageType.Free;
            CommitPage(target);
            target.OriginalPageId = -1;
        }

        /// <summary>
        /// Step along the free page list, adding new pages if needed
        /// </summary>
        [NotNull]private Page<FreeListPage> WalkFreeList([NotNull]Page<FreeListPage> freeLink)
        {
            // try to move forward in links
            if (freeLink.NextPageId > 0) {
                return GetPageView<FreeListPage>(freeLink.NextPageId) ?? throw new Exception("Free page list chain is broken");
            }
    
            // end of chain. Extend it.
            var newPage = ChainPage(freeLink, new FreeListPage().Freeze(), -1);
            return Page<FreeListPage>.FromRaw(newPage);
        }

        /// <summary>
        /// Add a new blank page at the end of a chain.
        /// New page will carry the same document ID, first page ID, and page type.
        /// </summary>
        /// <param name="basePage">End of a page chain.</param>
        /// <param name="optionalContent">Data bytes to insert, if any</param>
        /// <param name="contentLength">Length of data to use. To use entire buffer, you can pass -1</param>
        [NotNull]public ComplexPage ChainPage([CanBeNull] ComplexPage basePage, [CanBeNull]Stream optionalContent, int contentLength) {
            if (optionalContent == null) contentLength = 0;
            else if (contentLength > optionalContent.Length) throw new Exception("Page content length requested is outside of buffer provided");
            else if (contentLength < 0) contentLength = (int)optionalContent.Length; // allow `-1` for whole buffer

            var nextPageValue = ComplexPage.NextIdForEmptyPage + contentLength;

            if (basePage == null) {
                // special case -- make the first page of a doc
                // allowing this makes logic elsewhere a lot easier to follow
                var first = GetFreePage();
                first.PrevPageId = -1;
                first.DocumentId = Guid.NewGuid();
                first.FirstPageId = first.OriginalPageId;
                first.NextPageId = nextPageValue;
                first.PageType = PageType.Invalid;
                first.Dirty = true;
                first.DocumentSequence = 0;

                if (optionalContent != null) {
                    first.Write(optionalContent, 0, 0, contentLength);
                }
                CommitPage(first);
                return first;
            }

            if (basePage.OriginalPageId <= 0) throw new Exception("Tried to extend an invalid page");
            if (((int)basePage.PageType & (int)PageType.Free) == (int)PageType.Free) throw new Exception($"Tried to extend a freed page (State = {basePage.PageType.ToString()})");
            if (optionalContent?.Length > ComplexPage.PageDataCapacity) throw new Exception("New page content too large");

            var newPage = GetFreePage();
            newPage.PrevPageId = basePage.OriginalPageId;
            newPage.DocumentId = basePage.DocumentId;
            newPage.FirstPageId = basePage.FirstPageId;
            newPage.NextPageId = nextPageValue;
            newPage.PageType = basePage.PageType;
            newPage.Dirty = true;
            newPage.DocumentSequence = (ushort) (basePage.DocumentSequence + 1);

            if (optionalContent != null) {
                newPage.Write(optionalContent, 0, 0, contentLength);
            }
            CommitPage(newPage);

            basePage.NextPageId = newPage.OriginalPageId; // race condition risk!
            basePage.Dirty = true;
            CommitPage(basePage);

            return newPage;
        }

        /// <summary>
        /// Write a page into the storage stream. The PageID *MUST* be correct.
        /// This method is very thread sensitive
        /// </summary>
        public void CommitPage<T>(Page<T> page) where T : IStreamSerialisable, new()
        {
            page?.SyncView();
            CommitPage((ComplexPage)page);
        }

        /// <summary>
        /// Write a page into the storage stream. The PageID *MUST* be correct.
        /// This method is very thread sensitive
        /// </summary>
        public void CommitPage(ComplexPage page) {
            if (page == null || page.OriginalPageId < 0) throw new Exception("Attempted to commit an invalid page");
            page.UpdateCRC();

            var w = _storage.AcquireWriter();
            try {
                CommitPage(page, w);
            }
            finally {
                _storage.Release(ref w);
            }
        }

        /// <summary>
        /// Write a page into the storage stream. The PageID *MUST* be correct.
        /// This method is very thread sensitive
        /// </summary>
        private void CommitPage([NotNull]ComplexPage page, [NotNull]BinaryWriter w)
        {
            w.Seek(page.OriginalPageId * ComplexPage.PageRawSize, SeekOrigin.Begin);
            page.Freeze().CopyTo(w.BaseStream);
            page.Dirty = false;
        }

        /// <summary>
        /// Write a new document to data pages and the index.
        /// Returns new document ID.
        /// </summary>
        /// <param name="docDataStream">Stream to use as document source. It will be read from current position to end.</param>
        public Guid WriteDocument(Stream docDataStream)
        {
            if (docDataStream == null) throw new Exception("Tried to write a null stream to the database");

            // build new page chain for data:
            ComplexPage page = null;
            var buf = new byte[ComplexPage.PageDataCapacity];
            int bytes;
            while ((bytes = docDataStream.Read(buf,0,buf.Length)) > 0) {
                var next = ChainPage(page, new MemoryStream(buf), bytes);

                if (next.PageType == PageType.Invalid) { // first page
                    next.PageType = PageType.Data;
                    CommitPage(next);
                }

                page = next;
            }
            if (page == null) throw new Exception("Tried to write an empty stream to the database");

            // add to index
            WriteToIndex(page);

            // return ID
            return page.DocumentId;
        }

        /// <summary>
        /// Walk the index page list, try to find a place to insert this page.
        /// Throw if any duplicates found.
        /// </summary>
        private void WriteToIndex([NotNull]ComplexPage lastPage)
        {
            var index = GetIndexPageList();

            // Walk the index page list
            while (true)
            {
                var ok = index.View.TryInsert(lastPage.DocumentId, lastPage.OriginalPageId);
                if (ok) { 
                    CommitPage(index);
                    return;
                }

                // Failed to insert, walk the chain
                index = WalkIndexList(index, shouldAdd: true);
                if (index == null) throw new Exception("Failed to extend index chain");
            }
        }
        
        /// <summary>
        /// Step along the free page list, adding new pages if needed
        /// </summary>
        [CanBeNull]private Page<IndexPage> WalkIndexList([NotNull]Page<IndexPage> indexLink, bool shouldAdd)
        {
            // try to move forward in links
            if (indexLink.NextPageId > 0) {
                return GetPageView<IndexPage>(indexLink.NextPageId) ?? throw new Exception("Index page list chain is broken");
            }
    
            // end of chain. Extend it?
            if (!shouldAdd) return null;
            var newPage = ChainPage(indexLink, new IndexPage().Freeze(), -1);
            return Page<IndexPage>.FromRaw(newPage);
        }

        /// <summary>
        /// Search the index for a page ID. Returns -1 if not found.
        /// Returns only the most recent version
        /// </summary>
        public int GetPageIdFromDocumentId(Guid docId) {
            var index = GetIndexPageList();

            // Walk the index page list
            while (true)
            {
                var found = index.View.Search(docId, out var version);
                if (found) {
                    var ok = version.TryGetLink(0, out var pageId);
                    if (!ok) throw new Exception("Index version data was damaged");

                    return pageId;
                }

                // Failed to find, walk the chain
                index = WalkIndexList(index, shouldAdd: false);
                if (index == null) return -1; // not found
            }
        }

        /// <summary>
        /// Search the index for a versioned page ID. Returns null if not found.
        /// </summary>
        public VersionedLink GetDocumentVersions(Guid docId) {
            var index = GetIndexPageList();

            // Walk the index page list
            while (true)
            {
                var found = index.View.Search(docId, out var version);
                if (found) { return version; }

                // Failed to find, walk the chain
                index = WalkIndexList(index, shouldAdd: false);
                if (index == null) return null; // not found
            }
        }

        /// <summary>
        /// Present a stream to read from a document, recovered by ID.
        /// Returns null if the document is not found.
        /// </summary>
        [CanBeNull]public Stream ReadDocument(Guid docId)
        {
            var version = GetDocumentVersions(docId);
            if (version == null) return null; // not found

            if (version.TryGetLink(0, out var pageId)) {
                try {
                    return new PageTableStream(this, GetPageRaw(pageId), false);
                } catch {
                    if (version.TryGetLink(1, out pageId)) {
                        return new PageTableStream(this, GetPageRaw(pageId), false);
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Find a specific page in a chain, by document sequence number.
        /// If the chain does not contain this sequence, method will return null
        /// </summary>
        /// <param name="src">Any page in the chain, but preferably the end page</param>
        /// <param name="sequenceNumber">zero-based sequence number</param>
        [CanBeNull]public ComplexPage FindPageInChain(ComplexPage src, int sequenceNumber)
        {
            if (src == null) return null;

            // We should be able to walk around the pages relatively efficiently, assuming both links are good.
            // From any position, we can
            // 1. Jump to the start and walk forward
            // 2. Walk forward or backward from where we are
            // 3. Jump to the end and walk backward

            // For now, we do the dumb thing: 1. Jump to start; 2. walk forward N steps;

            var page = GetPageRaw(src.FirstPageId);
            for (int i = 0; i < sequenceNumber; i++)
            {
                page = WalkPageChain(page);
                if (page == null) {
                    throw new Exception($"Lost page when searching for sequence number: {i} of {sequenceNumber}");
                }
            }

            if (((int)page.PageType & (int)PageType.Free) == (int)PageType.Free) {
                throw new Exception($"Sequence number results in bad page: {sequenceNumber}");
            }

            return page;
        }

        /// <summary>
        /// Step forward in the page chain. Returns null if at the end.
        /// Does not extend chains.
        /// </summary>
        [CanBeNull]public ComplexPage WalkPageChain(ComplexPage page)
        {
            if (page == null) return null;
            if (page.NextPageId < 0) return null;
            return GetPageRaw(page.NextPageId);
        }

        /// <summary>
        /// Bind a document ID to a path. If there was an existing document in that path,
        /// its ID will be returned.
        /// </summary>
        public Guid BindPathToDocument(string path, Guid docId)
        {
            lock (_newPageLock)
            {
                var lookup = ReadPathIndex();

                var id = SerialGuid.Wrap(docId);
                var old = lookup.Add(path, id);

                // write back changes
                CommitPathIndexCache();

                if (old?.Value == docId)
                {
                    // no change
                    return Guid.Empty;
                }

                return old?.Value ?? Guid.Empty;
            }
        }
        
        /// <summary>
        /// Try to find a document ID for a given path.
        /// Returns empty guid if not found.
        /// There is no guarantee that the document will still be present in the page table. You will need to do a subsequent read.
        /// </summary>
        public Guid GetDocumentIdByPath(string path)
        {
            var lookup = ReadPathIndex();
            var sg = lookup.Get(path);
            return sg?.Value ?? Guid.Empty;
        }
        
        /// <summary>
        /// List all paths that match a document id
        /// </summary>
        [NotNull, ItemNotNull]public IEnumerable<string> ListPathsForDocument(Guid docId)
        {
            var lookup = ReadPathIndex();
            return lookup.GetPathsForEntry(docId);
        }

        private void CommitPathIndexCache()
        {
            if (_pathIndexCache == null) return;

            using (var newPathData = _pathIndexCache.Freeze())
            {
                var raw = GetFreePage();
                raw.NextPageId = ComplexPage.NextIdForEmptyPage;
                raw.PageType = PageType.PathLookup;
                raw.DocumentId = ComplexPage.PathLookupGuid;
                raw.FirstPageId = raw.OriginalPageId;
                CommitPage(raw);
                raw = GetPageRaw(raw.OriginalPageId);

                var newDoc = new PageTableStream(this, raw, true);
                newPathData.Rewind();
                newPathData.CopyTo(newDoc);

                var root = ReadRoot();
                var oldPathId = root.GetPathLookupBase();
                var newPathId = newDoc.GetEndPageId();
                root.PathLookupLink.WriteNewLink(newPathId, out var expired);
                CommitRootCache();

                // The 'expired' link may be an earlier page in the same chain. Don't delete the expired page if it's in the new chain.
                if (expired >= 3)
                {
                    DeletePageChain(expired, oldPathId, newPathId); // this is causing or triggering issues
                }
            }
        }

        /// <summary>
        /// Write the root cache back to storage.
        /// This should be done as soon as possible if the root page is changed.
        /// </summary>
        public void CommitRootCache()
        {
            var page = Page<RootPage>.FromRaw(GetPageRaw(0));
            if (_rootPageCache == null) return;
            page.View = _rootPageCache;

            CommitPage(page);
        }

        [NotNull]private ReverseTrie<SerialGuid> ReadPathIndex()
        {
            if (_pathIndexCache != null) return _pathIndexCache;
            var root = ReadRoot();
            var pathBaseId = root.GetPathLookupBase();
            
            // path index is forward-writing, so we need to find the end...
            var page = GetPageRaw(pathBaseId);

            // load as a stream
            var source = new PageTableStream(this, page, false);

            _pathIndexCache = new ReverseTrie<SerialGuid>();
            if (source.Length <= 0) throw new Exception($"Invalid path index data from pageId = {pathBaseId}");
            _pathIndexCache.Defrost(source);

            return _pathIndexCache;
        }


        /// <summary>
        /// Delete a document page chain. Does NOT directly affect the path index or document index
        /// </summary>
        public void DeleteDocument(Guid oldId)
        {
            var pageId = GetPageIdFromDocumentId(oldId);
            if (pageId > 3) DeletePageChain(pageId);
        }

        /// <summary>
        /// Unbind all paths for the given document ID.
        /// This does not delete the document page chain or update the document index
        /// </summary>
        public void DeletePathsForDocument(Guid documentId)
        {
            var lookup = ReadPathIndex();
            var found = lookup.GetPathsForEntry(documentId).ToArray();
            foreach (var path in found)
            {
                lookup.Delete(path);
            }
            CommitPathIndexCache();
        }

        /// <summary>
        /// Remove a document from the main index.
        /// You should also call `DeletePathsForDocument` and `DeleteDocument`
        /// </summary>
        /// <remarks>
        /// This really just marks the document as invalid. We might add some garbage collection later.
        /// </remarks>
        public void RemoveFromIndex(Guid documentId)
        {
            var index = GetIndexPageList();
            index.View.Update(documentId, -1, out _);
        }

        /// <summary>
        /// Delete a document page chain. Does NOT directly affect the path index or document index
        /// </summary>
        public void DeleteSinglePathForDocument(Guid docId, string path)
        {
            var lookup = ReadPathIndex();
            if (lookup.Get(path)?.Value != docId) return;

            lookup.Delete(path);
        }

        [NotNull, ItemNotNull]
        public IEnumerable<string> SearchPaths(string pathPrefix)
        {
            var lookup = ReadPathIndex();
            return lookup.Search(pathPrefix);
        }

        /// <summary>
        /// Scan the free page chain, count how many slots are occupied
        /// </summary>
        public int CountFreePages()
        {
            var count = 0;
            var freeList = GetFreePageList();
            
            // Walk the free page list
            while (freeList != null)
            {
                count += freeList.View.Count();
                if (freeList.NextPageId <= 0) break;
                freeList = GetPageView<FreeListPage>(freeList.NextPageId);
            }

            return count;
        }
    }
}