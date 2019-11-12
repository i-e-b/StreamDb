using System;
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
    /// </summary>
    public class PageStreamStorage {
        [NotNull] private readonly Stream _fs;
        [NotNull] private readonly object fslock = new object();

        /// <summary> A magic number we use to recognise our database format </summary>
        [NotNull] public static readonly byte[] HEADER_MAGIC = { 0x55, 0xAA, 0xFE, 0xED, 0xFA, 0xCE, 0xDA, 0x7A };

        public const int MAGIC_SIZE = 8;
        public const int HEADER_SIZE = (VersionedLink.ByteSize * 3) + MAGIC_SIZE;

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
            // TODO

            var bytesRequired = dataStream.Length - dataStream.Position;
            var pagesRequired = CeilDiv(bytesRequired, SimplePage.PageRawSize);

            var pages = new int[pagesRequired];
            lock (fslock) {
                for (int i = 0; i < pagesRequired; i++) { pages[i] = GetFreePage(); }
            }

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

        private int TEMP_FREE_IDX;
        /// <summary>
        /// Reserve a new page for use, and return its ID.
        /// This may allocate a new page
        /// </summary>
        public int GetFreePage()
        {
            // TODO: free page container and updates
            var idx = TEMP_FREE_IDX++;
            CommitPage(new SimplePage(idx));
            return idx;
        }

        /// <summary>
        /// Release all pages in a chain. They can be reused on next write
        /// </summary>
        public void ReleaseChain(int endPageId) {
            // TODO
        }

        /// <summary>
        /// Read a page from the storage stream to memory. This will check the CRC.
        /// </summary>
        public SimplePage GetRawPage(int pageId)
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
        

        private int CeilDiv(long bytes, int pageSize)
        {
            var full = bytes / pageSize;
            if (bytes % pageSize > 0) full++;
            return (int)full;
        }
    }
}