﻿using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.DbStructure;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.Core
{
    /// <summary>
    /// Represents a generalised page in the DB.
    /// At the moment these are fixed to 4kb for data + headers
    /// </summary>
    public class ComplexPage : IStreamSerialisable {

        /// <summary>
        /// Size of a page in storage, including all headers and data
        /// </summary>
        public const int PageRawSize = 4096; // 4k data, to fit in a typical VM page
        /// <summary>
        /// Size of page headers
        /// </summary>
        public const int PageHeadersSize = 35; // All the metadata for a page
        /// <summary>
        /// Maximum data capacity of a page
        /// </summary>
        public const int PageDataCapacity = PageRawSize - PageHeadersSize; // 4k data - 35 bytes of header
        /// <summary>
        /// Value for NextPageId if the page is empty
        /// </summary>
        public const int NextIdForEmptyPage = -1 - PageDataCapacity;

        /// <summary> Special ID for the root page / root document of the database </summary>
        public static readonly Guid RootDocumentGuid = new Guid(new byte[] {0x57,0x2e,0xfe,0xed,0xfa,0xce,0xda,0x7a, 0, 0, 0, 0, 0, 0, 0, 0 }); // matches `HEADER_MAGIC` in PageTable.cs
        /// <summary> Special ID for the index tree document </summary>
        public static readonly Guid IndexTreeGuid    = new Guid(new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 1, 1, 1, 1, 1, 1, 1, 1 });
        /// <summary> Special ID for the path lookup document </summary>
        public static readonly Guid PathLookupGuid   = new Guid(new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 2, 2, 2, 2, 2, 2, 2, 2 });
        /// <summary> Special ID for page that lists other pages that have been deleted </summary>
        public static readonly Guid FreePageGuid     = new Guid(new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 3, 3, 3, 3, 3, 3, 3, 3 });
        
        /// <summary> Special invalid GUID used for indexing </summary>
        public static readonly Guid InvalidGuid_Index     = new Guid(new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127 });


        // data positions
        private const int DOC_ID = 0;
        private const int FIRST_PAGE_ID = 16;
        private const int PAGE_TYPE = 20;
        private const int DOC_SEQ = 21;
        private const int PREV_LNK = 23;
        private const int NEXT_LNK = 27;
        private const int CRC_HASH = 31;
        private const int PAGE_DATA = 35;

        /*
         
       bits   bytes    Data layout:
        128      16    [DocID:        GUID] <-- all pages belonging to a document share the same ID. The root page has a special 'HEADER_MAGIC' value plus 8 bytes of zero.
        160      20    [FirstPageId: int32] <-- link directly back to the first page of the page chain (all pages in a chain will have the same first page id)
        168      21    [PageType:     byte] <-- what does the 'data' part represent?
        184      23    [DocSeq:     uint16] <-- position in the document (uint16 limits documents to 256MB each. Used for recovery)
        216      27    [Prev:        int32] <-- previous page in the sequence ( -1 if this is the start )
        248      31    [Next:        int32] <-- next page in the sequence ( negative if this is the end )
        280      35    [CRC32:      uint32] <-- CRC of the entire page (including headers)
      32768    4096    [data:   byte[4061]] <-- page contents (interpret based on PageType)

            */

        [NotNull] protected internal readonly byte[] _data;

        public ComplexPage() { _data = new byte[PageRawSize]; }

        /// <summary>
        /// All pages belonging to a document share the same ID.
        /// <para></para>
        /// There are a few special IDs: Page.RootDocumentGuid, Page.IndexTreeGuid, Page.PathLookupGuid, Page.FreePageGuid
        /// <para></para>
        /// Two other IDs are disallowed: Page.InvalidGuid_Index and Guid.Zero
        /// </summary>
        public Guid DocumentId {
            get { return new Guid(Slice(DOC_ID, 16)); }
            set { Unslice(value.ToByteArray(), DOC_ID); }
        }

        /// <summary>
        /// Link directly back to the first page of the page chain (all pages in a chain will have the same first page id)
        /// </summary>
        public int FirstPageId { 
            get { return BitConverter.ToInt32(_data, FIRST_PAGE_ID); } 
            set { Unslice(BitConverter.GetBytes(value), FIRST_PAGE_ID); }
        }

        /// <summary>
        /// What does the 'data' part represent? Also has a flag for 'free' status
        /// </summary>
        public PageType PageType { 
            get { return (PageType)_data[PAGE_TYPE]; } 
            set { _data[PAGE_TYPE] = (byte)value; }
        }
        
        /// <summary>
        /// position in the document (uint16 limits documents to 256MB each. Used for recovery)
        /// </summary>
        public ushort DocumentSequence {  
            get { return BitConverter.ToUInt16(_data, DOC_SEQ); } 
            set { Unslice(BitConverter.GetBytes(value), DOC_SEQ); }
        }
        
        /// <summary>
        /// previous page in the document's page chain ( -1 if this is the start )
        /// </summary>
        public int PrevPageId {
            get { return BitConverter.ToInt32(_data, PREV_LNK); } 
            set { Unslice(BitConverter.GetBytes(value), PREV_LNK); }
        }
        
        // TODO: The "NextPageId" should be protected from races somehow.
        /// <summary>
        /// next page in the sequence ( negative if this is the end )
        /// <para></para>
        /// If this is the last page in a chain, the exact negative value gives the length of data in the page.
        /// `-1` = page is full, `-2` = page is one byte short of full ... `-PageDataCapacity` = page contains only 1 byte
        /// </summary>
        public int NextPageId { 
            get { return BitConverter.ToInt32(_data, NEXT_LNK); } 
            set { Unslice(BitConverter.GetBytes(value), NEXT_LNK); }
        }
        
        /// <summary>
        /// CRC of the entire page (including headers).
        /// This is critical to the versioning and recovery process, so must be kept up-to-date
        /// </summary>
        public uint CrcHash { 
            get { return BitConverter.ToUInt32(_data, CRC_HASH); } 
            set { Unslice(BitConverter.GetBytes(value), CRC_HASH); }
        }

        /// <summary>
        /// Length of data in this page. Calculated from `NextPageId`. This is not written to storage.
        /// </summary>
        public int PageDataLength { get {
                if (NextPageId >= 0) return PageDataCapacity; // assume the page is full if it continues
                return PageDataCapacity + NextPageId + 1;
            }
        }

        /// <summary>
        /// A flag for use in caching. This is not written to storage.
        /// </summary>
        public bool Dirty { get; set; }

        /// <summary>
        /// Page ID that this instance was loaded from. This is not written to storage
        /// </summary>
        public int OriginalPageId { get; set; }

        [NotNull]private byte[] Slice(int start, int length) {
            var result = new byte[length];
            for (int i = 0; i < length; i++) { result[i] = _data[i+start]; }
            return result;
        }
        private void Unslice(byte[] input, int index) {
            if (input == null) return;
            for (int i = 0; i < input.Length; i++) { _data[index+i] = input[i]; }
        }

        /// <inheritdoc />
        public Stream Freeze() { return new MemoryStream(_data); }

        /// <inheritdoc />
        public void Defrost(Stream source)
        {
            if (source == null) throw new Exception("Page source was null");
            if (source.Length != PageRawSize) throw new Exception($"Page source was not the correct size. Expected {PageRawSize}, got {source.Length}");
            source.Read(_data, 0, PageRawSize);
        }

        public void UpdateCRC()
        {
            // We calculate the entire page (headers + data), but with the CRC field zeroed.
            CrcHash = 0;
            CrcHash = Crc32.Compute(_data);
        }

        public bool ValidateCrc()
        {
            var original = CrcHash;
            CrcHash = 0;
            var actual = Crc32.Compute(_data);
            CrcHash = original;

            return actual == original;
        }

        /// <summary>
        /// Copy data from a buffer into the data section of the page
        /// </summary>
        /// <param name="input">Input data</param>
        /// <param name="inputOffset">offset into the input data to start</param>
        /// <param name="pageOffset">offset into the page data</param>
        /// <param name="length">number of bytes to copy</param>
        public void Write(byte[] input, int inputOffset, int pageOffset, int length)
        {
            if (input == null) return;
            if (inputOffset + length > input.Length) throw new Exception("Page Write exceeds input size");
            if (pageOffset + length > PageDataCapacity) throw new Exception("Page Write exceeds page size");

            for (int i = 0; i < length; i++)
            {
                _data[PAGE_DATA + pageOffset + i] = input[inputOffset + i];
            }

            if (NextPageId < 0) {
                // adjust length
                var writeExtent = NextIdForEmptyPage + (pageOffset + length);
                NextPageId = Math.Max(NextPageId, writeExtent);
            }
        }
        
        /// <summary>
        /// Copy data from a buffer into the data section of the page
        /// </summary>
        /// <param name="input">Input data</param>
        /// <param name="inputOffset">offset into the input data to start</param>
        /// <param name="pageOffset">offset into the page data</param>
        /// <param name="length">number of bytes to copy</param>
        public void Write(Stream input, int inputOffset, int pageOffset, long length)
        {
            if (input == null) return;
            if (inputOffset + length > input.Length) throw new Exception("Page Write exceeds input size");
            if (pageOffset + length > PageDataCapacity) throw new Exception("Page Write exceeds page size");

            input.Read(_data, PAGE_DATA+pageOffset, (int)length);

            if (NextPageId < 0) {
                // adjust length
                int writeExtent = (int)(NextIdForEmptyPage + (pageOffset + length));
                NextPageId = Math.Max(NextPageId, writeExtent);
            }
        }

        /// <summary>
        /// Copy data from the data section of the page into a buffer
        /// </summary>
        /// <param name="buffer">data buffer</param>
        /// <param name="bufferOffset">offset into the buffer to start</param>
        /// <param name="pageOffset">offset into the page data</param>
        /// <param name="length">number of bytes to copy</param>
        public void Read(byte[] buffer, int bufferOffset, int pageOffset, int length)
        {
            if (buffer == null) return;
            if (bufferOffset + length > buffer.Length) throw new Exception("Page Read exceeds buffer size");
            if (pageOffset + length > PageDataCapacity) throw new Exception("Page Read exceeds page size");

            for (int i = 0; i < length; i++)
            {
                buffer[i + bufferOffset] = _data[PAGE_DATA + pageOffset + i];
            }
        }

        /// <summary>
        /// Get a stream over the page data.
        /// This may be offset to start at the contents, so be careful with seeking
        /// </summary>
        [NotNull]public Stream GetDataStream() {
            var ms = new MemoryStream(_data);
            ms.Seek(PAGE_DATA, SeekOrigin.Begin);
            return ms;
        }

        /// <summary>
        /// Get a COPY of the pages's content data. Does not include headers.
        /// </summary>
        [NotNull]public byte[] GetContentCopy()
        {
            return Slice(PAGE_DATA, PageDataCapacity);
        }

        /// <summary>
        /// Read a single byte from the page content
        /// </summary>
        public byte GetByte(int offset)
        {
            return _data[PAGE_DATA + offset];
        }

        /// <summary>
        /// Return the entire page data (headers and body). No copying is done.
        /// </summary>
        [NotNull]public byte[] RawData()
        {
            return _data;
        }
    }
}