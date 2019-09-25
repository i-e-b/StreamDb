using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.DbStructure
{
    /// <summary>
    /// This is the core of the database format.
    /// Our database starts with a root page that links to:
    ///  - an index page chain
    ///  - a path lookup chain
    ///  - a free page chain
    /// Occupied document data pages are not listed in the root, these are reachable from the index pages.
    /// A valid page table never has less than 4 pages (pageID 0..3) -- so pageID 4 is the first valid page to allocate
    /// </summary>
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

        public PageTable([NotNull]Stream fs)
        {
            fs.Seek(0, SeekOrigin.Begin);
            _storage = new InterlockBinaryStream(fs, false);
            if (fs.Length == 0) // Empty. Initialise with a root page
            {
                InitialisePageTable();
            }
            else if (fs.Length < Page.PageRawSize) // can't have a root page
            {
                if (fs.Length >= 8)
                { // has some data
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
            // assume it is ok...
            //ReadPageTable();
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

            var root = NewRootForBlankDB(root0);
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

        [NotNull]private static Page NewRootForBlankDB([NotNull]RootPage root0)
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
        [CanBeNull]public Page GetPage(int pageId)
        {
            var reader = _storage.AcquireReader();
            try {
                if (reader.BaseStream == null) throw new Exception("Page table base stream is invalid");
                if (pageId < 0) return null;

                var byteOffset = pageId * Page.PageRawSize;
                var byteEnd = byteOffset + Page.PageRawSize;

                if (reader.BaseStream.Length < byteEnd) return null;

                var result = new Page();
                reader.BaseStream.Seek(byteOffset, SeekOrigin.Begin);
                result.FromBytes(reader.ReadBytes(Page.PageRawSize));
                result.OriginalPageId = pageId;
                return result;
            } finally {
                _storage.Release(ref reader);
            }
        }

        [NotNull]private RootPage ReadRoot() {
            // TODO: avoid re-reading every time
            var page = GetPage(0);
            if (page == null) throw new Exception("PageTable.ReadRoot: Failed to read root page");
            if (!page.ValidateCrc()) throw new Exception("PageTable.ReadRoot: Root entry failed CRC check. Must recover database.");
            var final = new RootPage();
            final.FromBytes(page.GetData());
            return final;
        }

        [NotNull]private FreeListPage GetFreePageList() {
            // TODO: avoid re-reading every time
            var root = ReadRoot();
            var page = GetPage(root.GetFreeListPageId());
            if (page == null) throw new Exception("PageTable.ReadRoot: Failed to free list page");
            var free = new FreeListPage();
            free.FromBytes(page.GetData());
            return free;
        }

        /// <summary>
        /// Get a free page, either by reuse or by allocating a new page.
        /// The returned page will have a correct OriginalPageID, but will need other headers reset, and a new CRC before being committed
        /// </summary>
        public Page GetFreePage()
        {
            // try to re-use a page:
            var freeList = GetFreePageList();
            var pageId = freeList.GetNext();
            if (pageId > 0) { return GetPage(pageId); }

            // none available. Write a new one:
            lock (_newPageLock)
            {
                var w = _storage.AcquireWriter();
                try
                {
                    var stream = w.BaseStream ?? throw new Exception("Lost connection to base stream");
                    var pageCount = stream.Length / Page.PageRawSize;

                    var p = new Page
                    {
                        PageType = PageType.Invalid,
                        NextPageId = -1,
                        PrevPageId = -1,
                        FirstPageId = (int)pageCount, // very thread sensitive!
                        DocumentSequence = 0,
                        DocumentId = Guid.Empty
                    };
                    p.UpdateCRC();

                    // TODO: add to free list?

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
        /// Write a page into the storage stream. The PageID *MUST* be correct.
        /// This method is very thread sensitive
        /// </summary>
        public void CommitPage(Page page) {
            if (page == null || page.FirstPageId < 0) throw new Exception("Attempted to commit an invalid page");
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
            w.Seek(page.FirstPageId * Page.PageRawSize, SeekOrigin.Begin);
            var buf = page.ToBytes();
            w.Write(buf);
        }
    }
}