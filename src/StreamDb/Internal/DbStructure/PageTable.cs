﻿using System;
using System.Collections.Generic;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.DbStructure
{
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
    public class PageTable
    {
        /// <summary> A magic number we use to recognise our database format </summary>
        public const ulong HEADER_MAGIC = 0x572E_FEED_FACE_DA7A;

        /*
         * Page table should have a single root entry that is not updated unless absolutely needed (helps with thread safety)
         * This should have links to:
         *   - a GUID tree (for document ID -> Page lookups); Guid tree nodes should have a list of versions, each to a chain of data
         *   - Data page list
         * maybe more, but see if we can cope with just this
         */
        [NotNull]private readonly InterlockBinaryStream _storage;
        
        /// <summary>
        /// A lock around getting a new page
        /// </summary>
        [NotNull]private readonly object _newPageLock = new object();

        [CanBeNull] private RootPage _rootPageCache = null;
        [CanBeNull] private PathIndex<SerialGuid> _pathIndexCache = null;

        public PageTable([NotNull]Stream fs)
        {
            fs.Seek(0, SeekOrigin.Begin);
            _storage = new InterlockBinaryStream(fs, false);

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
            else if (fs.Length < (Page.PageRawSize*3)) // can't have a valid structure
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
            var path0 = new PathIndex<SerialGuid>();

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
                w.Write(root.ToBytes());
                w.Write(index.ToBytes());
                w.Write(free.ToBytes());
                w.Write(path.ToBytes());
            }
            finally 
            {
                _storage.Release(ref w);
            }

        }
        
        [NotNull]private static Page NewPathLookupForBlankDB([NotNull]PathIndex<SerialGuid> path0)
        {
            var page = new Page
            {
                PageType = PageType.PathLookup,
                NextPageId = -1,
                PrevPageId = -1,
                DocumentSequence = 0,
                DocumentId = Page.PathLookupGuid,
                Dirty = true,
                FirstPageId = 3
            };
            var bytes = path0.ToBytes();
            page.Write(bytes, 0, 0, bytes.Length);
            page.UpdateCRC();
            return page;
        }
        
        [NotNull]private static Page NewFreeListForBlankDB([NotNull]FreeListPage free0)
        {
            var page = new Page
            {
                PageType = PageType.ExpiredList,
                NextPageId = -1,
                PrevPageId = -1,
                DocumentSequence = 0,
                DocumentId = Page.FreePageGuid,
                Dirty = true,
                FirstPageId = 2
            };
            var bytes = free0.ToBytes();
            page.Write(bytes, 0, 0, bytes.Length);
            page.UpdateCRC();
            return page;
        }

        [NotNull]private static Page NewIndexForBlankDB([NotNull]IndexPage index0)
        {
            var index = new Page
            {
                PageType = PageType.Index,
                NextPageId = -1,
                PrevPageId = -1,
                DocumentSequence = 0,
                DocumentId = Page.IndexTreeGuid,
                Dirty = true,
                FirstPageId = 1
            };
            var indexBytes = index0.ToBytes();
            index.Write(indexBytes, 0, 0, indexBytes.Length);
            index.UpdateCRC();
            return index;
        }

        [NotNull]private static Page NewRootPage([NotNull]RootPage root0)
        {
            var root = new Page
            {
                PageType = PageType.Root,
                NextPageId = -1,
                PrevPageId = -1,
                DocumentSequence = 0,
                DocumentId = Page.RootDocumentGuid,
                Dirty = true,
                FirstPageId = 0
            };
            var rootBytes = root0.ToBytes();
            root.Write(rootBytes, 0, 0, rootBytes.Length);
            root.UpdateCRC();
            return root;
        }

        /// <summary>
        /// Read a specific existing page by Page ID. Returns null if the page does not exist.
        /// </summary>
        [CanBeNull]public Page<T> GetPageView<T>(int pageId) where T : IByteSerialisable, new()
        {
            var reader = _storage.AcquireReader();
            try {
                if (reader.BaseStream == null) throw new Exception("Page table base stream is invalid");
                if (pageId < 0) return null;

                var byteOffset = pageId * Page.PageRawSize;
                var byteEnd = byteOffset + Page.PageRawSize;

                if (reader.BaseStream.Length < byteEnd) return null;

                reader.BaseStream.Seek(byteOffset, SeekOrigin.Begin);
                var bytes = reader.ReadBytes(Page.PageRawSize);
                return new Page<T>(pageId, bytes);
            } finally {
                _storage.Release(ref reader);
            }
        }
        [CanBeNull]public Page GetPageRaw(int pageId)
        {
            var reader = _storage.AcquireReader();
            try {
                if (reader.BaseStream == null) throw new Exception("Page table base stream is invalid");
                if (pageId < 0) return null;

                var byteOffset = pageId * Page.PageRawSize;
                var byteEnd = byteOffset + Page.PageRawSize;

                if (reader.BaseStream.Length < byteEnd) return null;

                reader.BaseStream.Seek(byteOffset, SeekOrigin.Begin);
                var bytes = reader.ReadBytes(Page.PageRawSize);
                
                var result = new Page();
                result.FromBytes(bytes);
                result.OriginalPageId = pageId;
                if (!result.ValidateCrc()) return null;
                return result;
            } finally {
                _storage.Release(ref reader);
            }
        }

        [NotNull]private RootPage ReadRoot() {
            if (_rootPageCache != null) return _rootPageCache;

            var page = GetPageRaw(0);
            if (page == null) throw new Exception("PageTable.ReadRoot: Failed to read root page");
            if (!page.ValidateCrc()) throw new Exception("PageTable.ReadRoot: Root entry failed CRC check. Must recover database.");

            var final = new RootPage();
            final.FromBytes(page.GetData());
            _rootPageCache = final;
            return final;
        }

        [NotNull]private Page<FreeListPage> GetFreePageList() {
            // TODO: avoid re-reading every time
            var root = ReadRoot();
            var page = GetPageView<FreeListPage>(root.GetFreeListPageId());
            if (page == null) throw new Exception("PageTable.ReadRoot: Failed to free list page");
            return page;
        }
        
        [NotNull]private Page<IndexPage> GetIndexPageList() {
            // TODO: avoid re-reading every time
            var root = ReadRoot();
            var page = GetPageView<IndexPage>(root.GetIndexListId());
            if (page == null) throw new Exception("PageTable.ReadRoot: Failed to free list page");
            return page;
        }

        /// <summary>
        /// Get a free page, either by reuse or by allocating a new page.
        /// The returned page will have a correct OriginalPageID, but will need other headers reset, and a new CRC before being committed
        /// </summary>
        [NotNull]public Page GetFreePage()
        {
            // try to re-use a page:
            var freeList = GetFreePageList();

            // Walk the free page list
            while (true)
            {
                var pageId = freeList.View.GetNext();
                if (pageId > 0) { 
                    var page = GetPageRaw(pageId);
                    if (page == null) break;
                    return page;
                }
                if (freeList.NextPageId <= 0) break;

                // go to next free table page
                freeList = GetPageView<FreeListPage>(freeList.NextPageId);
                // TODO: if we empty one of the free pages, it should itself be freed.
                if (freeList == null) break;
            }

            // none available. Write a new one:
            return AllocatePage();
        }

        /// <summary>
        /// Allocate a new page (without checking for a free page)
        /// </summary>
        [NotNull]private Page AllocatePage()
        {
            lock (_newPageLock)
            {
                var w = _storage.AcquireWriter();
                try
                {
                    var stream = w.BaseStream ?? throw new Exception("Lost connection to base stream");
                    var pageCount = stream.Length / Page.PageRawSize;

                    var p = new Page
                    {
                        OriginalPageId = (int) pageCount, // very thread sensitive!
                        PageType = PageType.Invalid,
                        NextPageId = -1,
                        PrevPageId = -1,
                        FirstPageId = (int) pageCount, // should update if chaining
                        DocumentSequence = 0,
                        DocumentId = Guid.Empty
                    };
                    p.UpdateCRC();

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
        public void DeletePageChain(int pageId) {
            // TODO: Check the integrity of the chain before deleting

            if (pageId < 3) throw new Exception("Tried to delete a core page"); // note: the page lookup table can get entirely re-written, so it's not protected
            var free = GetFreePageList();

            var tagged = GetPageRaw(pageId);
            if (tagged == null) return;

            var rootId = tagged.FirstPageId;
            var current = GetPageRaw(rootId);

            var loopHash = new HashSet<int>();

            while (current != null) {
                var ok = free.View.TryAdd(current.OriginalPageId);
                loopHash.Add(current.OriginalPageId);

                if (!ok) {
                    // need to grow free list
                    free = WalkFreeList(free);
                } else {
                    // commit back to storage
                    free.UpdateCRC();
                    CommitPage(free);
                }

                if (loopHash.Contains(current.NextPageId)) throw new Exception("Loop detected in page list.");
                current = GetPageRaw(current.NextPageId);
            }
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
            var newPage = ChainPage(freeLink, new FreeListPage().ToBytes());
            return Page<FreeListPage>.FromRaw(newPage);
        }

        /// <summary>
        /// Add a new blank page at the end of a chain.
        /// New page will carry the same document ID, first page ID, and page type.
        /// </summary>
        /// <param name="other">End of a page chain.</param>
        /// <param name="optionalContent">Data bytes to insert, if any</param>
        [NotNull]public Page ChainPage([CanBeNull]Page other, [CanBeNull]byte[] optionalContent) {
            if (other == null) {
                // special case -- make the first page of a doc
                // allowing this makes logic elsewhere a lot easier to follow
                var first = AllocatePage();
                first.PrevPageId = -1;
                first.DocumentId = Guid.NewGuid();
                first.FirstPageId = first.OriginalPageId;
                first.NextPageId = -1;
                first.PageType = PageType.Invalid;
                first.Dirty = true;
                first.DocumentSequence = 0;

                if (optionalContent != null) {
                    first.Write(optionalContent, 0, 0, optionalContent.Length);
                }
                first.UpdateCRC();

                CommitPage(first);
                return first;
            }

            if (other.OriginalPageId <= 0) throw new Exception("Tried to extend an invalid page");
            if (((int)other.PageType & (int)PageType.Free) == (int)PageType.Free) throw new Exception("Tried to extend a freed page");
            if (optionalContent?.Length > Page.PageDataCapacity) throw new Exception("New page content too large");

            var newPage = AllocatePage();
            newPage.PrevPageId = other.OriginalPageId;
            newPage.DocumentId = other.DocumentId;
            newPage.FirstPageId = other.FirstPageId;
            newPage.NextPageId = -1;
            newPage.PageType = other.PageType;
            newPage.Dirty = true;
            newPage.DocumentSequence = (ushort) (other.DocumentSequence + 1);

            if (optionalContent != null) {
                newPage.Write(optionalContent, 0, 0, optionalContent.Length);
            }
            newPage.UpdateCRC();

            CommitPage(newPage);

            other.NextPageId = newPage.OriginalPageId; // race condition risk!
            other.Dirty = true;
            other.UpdateCRC();
            CommitPage(other);

            return newPage;
        }

        /// <summary>
        /// Write a page into the storage stream. The PageID *MUST* be correct.
        /// This method is very thread sensitive
        /// </summary>
        public void CommitPage<T>(Page<T> page) where T : IByteSerialisable, new()
        {
            page?.SyncView();
            CommitPage((Page)page);
        }

        /// <summary>
        /// Write a page into the storage stream. The PageID *MUST* be correct.
        /// This method is very thread sensitive
        /// </summary>
        public void CommitPage(Page page) {
            if (page == null || page.OriginalPageId < 0) throw new Exception("Attempted to commit an invalid page");
            if (!page.ValidateCrc()) throw new Exception("Attempted to commit a corrupted page");

            // TODO: remove from free list?

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
        private void CommitPage([NotNull]Page page, [NotNull]BinaryWriter w)
        {
            w.Seek(page.OriginalPageId * Page.PageRawSize, SeekOrigin.Begin);
            var buf = page.ToBytes();
            w.Write(buf);
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
            Page page = null;
            var buf = new byte[Page.PageDataCapacity];
            while (docDataStream.Read(buf,0,buf.Length) > 0) {
                var next = ChainPage(page, buf);

                if (next.PageType == PageType.Invalid) { // first page
                    next.PageType = PageType.Data;
                    next.UpdateCRC();
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
        private void WriteToIndex([NotNull]Page lastPage)
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
            var newPage = ChainPage(indexLink, new IndexPage().ToBytes());
            return Page<IndexPage>.FromRaw(newPage);
        }

        /// <summary>
        /// Search the index for a page ID. Returns -1 if not found.
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
        /// Present a stream to read from a document, recovered by ID.
        /// Returns null if the document is not found.
        /// </summary>
        [CanBeNull]public Stream ReadDocument(Guid docId)
        {
            // walk the index page list, try to find an end page for the given ID.
            // then skip to the start, and wrap in a page-reading stream implementation

            var pageId = GetPageIdFromDocumentId(docId);
            if (pageId < 4) return null; // don't read invalid or core pages this way
            return new PageTableStream(this, GetPageRaw(pageId));
        }

        /// <summary>
        /// Find a specific page in a chain, by document sequence number.
        /// If the chain does not contain this sequence, method will return null
        /// </summary>
        /// <param name="src">Any page in the chain, but preferably the end page</param>
        /// <param name="sequenceNumber">zero-based sequence number</param>
        [CanBeNull]public Page FindPageInChain(Page src, int sequenceNumber)
        {
            if (src == null) return null;

            // TODO:
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
            }

            return page;
        }

        /// <summary>
        /// Step forward in the page chain. Returns null if at the end.
        /// Does not extend chains.
        /// </summary>
        [CanBeNull]public Page WalkPageChain(Page page)
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

                if (old?.Value == docId)
                {
                    // no change
                    return Guid.Empty;
                }

                // write back changes
                CommitPathIndexCache();

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

        private void CommitPathIndexCache()
        {
            // TODO: extend the path index chain with the NEW data in the lookup object.
            // This current way is pretty messy.

            if (_pathIndexCache == null) return;
            using (var data = new MemoryStream(_pathIndexCache.ToBytes()))
            {
                data.Seek(0, SeekOrigin.Begin);

                // build new page chain for data:
                Page page = null;
                var buf = new byte[Page.PageDataCapacity];
                while (data.Read(buf, 0, buf.Length) > 0)
                {
                    var next = ChainPage(page, buf);

                    if (next.PageType == PageType.Invalid)
                    { // first page
                        next.PageType = PageType.PathLookup;
                        next.DocumentId = Page.PathLookupGuid;
                        next.UpdateCRC();
                        CommitPage(next);
                    }

                    page = next;
                }
                if (page == null) throw new Exception("Failed to commit path index cache to database");

                // Now update the root page
                var root = ReadRoot();
                root.AddPathLookup(page.FirstPageId, out var expired);
                CommitRootCache(); // <-- would like to avoid this by having an append-only path lookup
                if (expired > 0) {
                    DeletePageChain(expired);
                }
            }
        }

        /// <summary>
        /// Write the root cache back to storage.
        /// This should be done as soon as possible if the root page is changed.
        /// </summary>
        private void CommitRootCache()
        {
            if (_rootPageCache == null) return;

            var page = NewRootPage(_rootPageCache);
            
            CommitPage(page);
        }

        [NotNull]private PathIndex<SerialGuid> ReadPathIndex()
        {
            if (_pathIndexCache != null) return _pathIndexCache;
            var root = ReadRoot();
            var pathBaseId = root.GetPathLookupBase();
            
            var source = new PageTableStream(this, GetPageRaw(pathBaseId));

            _pathIndexCache = PathIndex<SerialGuid>.ReadFrom(source);

            return _pathIndexCache;
        }
    }
}