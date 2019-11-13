using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.Core
{
    /// <summary>
    /// Represents a general purpose page in the DB.
    /// These are fixed size.
    /// </summary><remarks>
    /// The simplified page contains only reverse links. Technically it's less recoverable than the complex page form in the case of partial destruction.
    /// </remarks>
    public class SimplePage : IStreamSerialisable {
        
        /// <summary>
        /// Size of a page in storage, including all headers and data
        /// </summary>
        public const int PageRawSize = 4096; // 4k data, to fit in a typical VM page
        /// <summary>
        /// Size of page headers
        /// </summary>
        public const int PageHeadersSize = 12; // All the metadata for a page
        /// <summary>
        /// Maximum data capacity of a page
        /// </summary>
        public const int PageDataCapacity = PageRawSize - PageHeadersSize;

        /// <summary>
        /// Maximum index that can be used
        /// </summary>
        public const int MaxInt32Index = (PageDataCapacity / 4) - 1;

        /*
         
       bits   bytes    Data layout:
         32       4    [CRC32:       int32] <-- CRC of the entire page (including headers)
         64       8    [Length:      int32] <-- length of data stored in body
         96      12    [Prev:       uint32] <-- previous page in the sequence ( -1 if this is the start )
      32768    4096    [data:   byte[4084]] <-- page contents (interpret based on PageType)

            */
            
        private const int CRC_HASH = 0;
        private const int DATA_LEN = 4;
        private const int PREV_LNK = 8;
        private const int PAGE_DATA = 12;
            
        /// <summary>
        /// Previous page in the document's page chain ( -1 if this is the start )
        /// </summary>
        public int PrevPageId {
            get {
                return ReadInt32(PREV_LNK);
            }
            set { WriteInt32(PREV_LNK, value); }
        }
        
        /// <summary>
        /// CRC of the entire page (including headers).
        /// </summary>
        public uint CrcHash { 
            get {
                return (uint) ReadInt32(CRC_HASH);
            }
            set { WriteInt32(CRC_HASH, (int)value); }
        }
        
        /// <summary>
        /// Length of data used inside this page
        /// </summary>
        public uint DataLength { 
            get {
                return (uint) ReadInt32(DATA_LEN);
            }
            set { WriteInt32(DATA_LEN, (int)value); }
        }

        /// <summary>
        /// Page ID that this instance was loaded from. This is not written to storage
        /// </summary>
        public int OriginalPageId { get; set; }

        [NotNull] protected internal readonly byte[] _data;

        public SimplePage(int pageId) { 
            _data = new byte[PageRawSize];
            OriginalPageId = pageId;
            DataLength = 0;
            PrevPageId = -1;
        }

        /// <inheritdoc />
        public Stream Freeze() { return new MemoryStream(_data); }

        /// <inheritdoc />
        public void Defrost(Stream source)
        {
            if (source == null) throw new Exception("Page source was null");
            var available = source.Length - source.Position;
            if (available < PageRawSize) throw new Exception($"Page source is not large enough to load a page. Expected at least{PageRawSize}, got {available}");
            source.Read(_data, 0, PageRawSize);
        }

        
        /// <summary>
        /// Return the number of pages required to store a given amount of data
        /// </summary>
        /// <param name="bytes">Number of bytes to store</param>
        /// <returns>Pages required</returns>
        public static int CountRequired(long bytes)
        {
            var full = bytes / PageDataCapacity;
            if (bytes % PageDataCapacity > 0) full++;
            return (int)full;
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

            var writeExtent = pageOffset + length;
            DataLength = (uint) Math.Max(DataLength, writeExtent);
        }
        
        /// <summary>
        /// Copy data from a buffer into the data section of the page
        /// </summary>
        /// <param name="input">Input data</param>
        /// <param name="pageOffset">offset into the page data</param>
        /// <param name="length">number of bytes to copy</param>
        public void Write(Stream input, int pageOffset, long length)
        {
            if (input == null) return;
            if (pageOffset + length > PageDataCapacity) throw new Exception("Page Write exceeds page size");

            var actual = input.Read(_data, PAGE_DATA+pageOffset, (int)length);

            var writeExtent = pageOffset + actual;
            DataLength = (uint) Math.Max(DataLength, writeExtent);
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

        
        private void WriteInt32(int baseAddr, int value)
        {
            _data[baseAddr + 0] = (byte) ((value >> 24) & 0xff);
            _data[baseAddr + 1] = (byte) ((value >> 16) & 0xff);
            _data[baseAddr + 2] = (byte) ((value >> 8) & 0xff);
            _data[baseAddr + 3] = (byte) ((value >> 0) & 0xff);
        }

        private int ReadInt32(int baseAddr) { return (_data[baseAddr + 0] << 24) + (_data[baseAddr + 1] << 16) + (_data[baseAddr + 2] << 8) + (_data[baseAddr + 3] << 0); }

        /// <summary>
        /// Treat the page data as an array of Int32. Read from an index
        /// </summary>
        public int ReadDataInt32(int idx) {
            if (idx < 0 || idx > MaxInt32Index) throw new Exception("Index out of range");
            var baseAddr = PAGE_DATA + (idx * 4);
            return (_data[baseAddr + 0] << 24) + (_data[baseAddr + 1] << 16) + (_data[baseAddr + 2] << 8) + (_data[baseAddr + 3] << 0);
        }
        
        /// <summary>
        /// Treat the page data as an array of Int32. Write to an index
        /// </summary>
        public void WriteDataInt32(int idx, int value)
        {
            if (idx < 0 || idx > MaxInt32Index) throw new Exception("Index out of range");
            var baseAddr = PAGE_DATA + (idx * 4);
            _data[baseAddr + 0] = (byte) ((value >> 24) & 0xff);
            _data[baseAddr + 1] = (byte) ((value >> 16) & 0xff);
            _data[baseAddr + 2] = (byte) ((value >> 8) & 0xff);
            _data[baseAddr + 3] = (byte) ((value >> 0) & 0xff);
        }

        /// <summary>
        /// Set all content data bytes to zero
        /// </summary>
        public void ZeroAllData()
        {
            for (int i = PAGE_DATA; i < _data.Length; i++)
            {
                _data[i] = 0;
            }
        }
    }
}