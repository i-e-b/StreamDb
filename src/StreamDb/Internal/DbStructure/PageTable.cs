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
            var w = _storage.AcquireWriter();
            try {
                w.Seek(0, SeekOrigin.Begin);
                var root = new Page{
                    PageType = PageType.Root,
                    RootPageId = 0,
                    DocumentId = Page.RootDocumentGuid,
                    DocumentSequence = 0,
                    NextPageId = -1,
                    PrevPageId = -1
                };
                root.UpdateCRC();

                var index1 = new Page{
                    PageType = PageType.Index,
                    RootPageId = 1,
                    DocumentId = Page.IndexTreeGuid,
                    DocumentSequence = 0,
                    NextPageId = -1,
                    PrevPageId = -1
                };
                index1.UpdateCRC();

                w.Write(root.ToBytes());
                w.Write(index1.ToBytes());
            }
            finally 
            {
                _storage.Release(ref w);
            }
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
                return result;
            } finally {
                _storage.Release(ref reader);
            }
        }

        /// <summary>
        /// Get a free page, either by reuse or by allocating a new page.
        /// The returned page will have a correct PageID, but will need other headers reset, and a new CRC before being committed
        /// </summary>
        public Page GetFreePage()
        {
            // TODO: read indexes to get a new page, rather than just making one.
            var w = _storage.AcquireWriter();
            try {
                var stream = w.BaseStream ?? throw new Exception("Lost connection to base stream");
                var pageCount = stream.Length / Page.PageRawSize;

                var p = new Page {
                    PageType = PageType.Invalid,
                    NextPageId = -1,
                    PrevPageId = -1,
                    RootPageId = (int) pageCount, // very thread sensitive!
                    DocumentSequence = 0,
                    DocumentId = Guid.Empty
                };
                p.UpdateCRC();

                CommitPage(p, w);
                return p;
            }
            finally {
                _storage.Release(ref w);
            }
        }

        /// <summary>
        /// Write a page into the storage stream. The PageID *MUST* be correct.
        /// This method is very thread sensitive
        /// </summary>
        public void CommitPage(Page page) {
            var w = _storage.AcquireWriter();
            try {
                if (page == null || page.RootPageId < 0) throw new Exception("Attempted to commit an invalid page");
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
            if (!page.ValidateCrc()) throw new Exception("Attempted to commit a corrupted page");

            w.Seek(page.RootPageId * Page.PageRawSize, SeekOrigin.Begin);
            var buf = page.ToBytes();
            w.Write(buf);
        }
    }
}