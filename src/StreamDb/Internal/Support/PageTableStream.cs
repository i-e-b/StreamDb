using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.DbStructure;

namespace StreamDb.Internal.Support
{
    /// <summary>
    /// Stream implementation that reads from a document chain in a PageTable
    /// </summary>
    public class PageTableStream : Stream {
        [NotNull]private readonly PageTable _parent;
        [NotNull]private readonly Page _endPage;
        [CanBeNull]private Page _mostRecentPage; // used to reduce scanning
        private readonly bool _enableWriting;
        private long _requestedOffset;

        /// <summary>
        /// Wrap a page chain in a stream reader.
        /// </summary>
        /// <param name="parent">PageTable to be used for traversal and reading</param>
        /// <param name="endPage">The LAST page of the page chain</param>
        /// <param name="enableWriting">If true, writing to the stream is enabled. This is for internal use only.</param>
        public PageTableStream(PageTable parent, Page endPage, bool enableWriting)
        {
            _requestedOffset = 0;
            _parent = parent ?? throw new Exception("Tried to stream from a disconnected page (PageTable parent is required)");
            _endPage = endPage ?? throw new Exception("Tried to stream from a null page");
            _enableWriting = enableWriting;
        }

        public override void Flush() { }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (count <= 0) return 0;
            if (buffer == null) return 0;
            if (offset + count > buffer.Length) throw new Exception("Requested data exceeds buffer capacity");

            var pageNumber = (int)(_requestedOffset / Page.PageDataCapacity);
            var chunkOffset = (int)(_requestedOffset % Page.PageDataCapacity);

            if (pageNumber < 0 || pageNumber > _endPage.DocumentSequence) return 0; // off the ends

            var page = FindPage(pageNumber);
            if (page == null) return 0; // out of bounds

            var remaining = count;
            var read = 0;
            while (remaining > 0)
            {
                var data = page.GetData();

                var chunkEnd = Math.Min(page.PageDataLength, chunkOffset + remaining);
                for (int i = chunkOffset; i < chunkEnd; i++)
                {
                    buffer[read+offset] = data[i];
                    read++;
                    remaining--;
                    _requestedOffset++;
                }

                chunkOffset = 0;
                // step page
                page = _parent.WalkPageChain(page);
                if (page == null) break;
            }
            _mostRecentPage = page;
            return read;
        }

        private Page FindPage(int pageNumber)
        {
            if (_mostRecentPage?.DocumentSequence == pageNumber) { return _mostRecentPage; }
            return _parent.FindPageInChain(_endPage, pageNumber);
        }

        /// <summary>
        /// Seek to a specific position for next read. Seek from end is not supported.
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    _requestedOffset = offset;
                    return _requestedOffset;

                case SeekOrigin.Current:
                    _requestedOffset += offset;
                    return _requestedOffset;

                case SeekOrigin.End:
                    _requestedOffset = Length + offset;
                    return _requestedOffset;

                default: throw new Exception("Non exhaustive switch");
            }
        }

        public override void SetLength(long value) { throw new Exception("Page content streams are read-only"); }

        /// <summary>Writes a sequence of bytes to the current stream and advances the current position within this stream by the number of bytes written.</summary>
        /// <param name="buffer">An array of bytes. This method copies <paramref name="count" /> bytes from <paramref name="buffer" /> to the current stream. </param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer" /> at which to begin copying bytes to the current stream. </param>
        /// <param name="count">The number of bytes to be written to the current stream. </param>
        public override void Write(byte[] buffer, int offset, int count) { 
            if (!_enableWriting) throw new Exception("Page content streams are read-only");

            if (count <= 0) return;
            if (buffer == null) return;
            if (offset + count > buffer.Length) throw new Exception("Requested data exceeds buffer capacity");

            var pageNumber = (int)(_requestedOffset / Page.PageDataCapacity);
            var chunkOffset = (int)(_requestedOffset % Page.PageDataCapacity);

            if (pageNumber < 0 || pageNumber > _endPage.DocumentSequence) return; // off the ends
            // we don't support seeking outside the existing range

            var page = FindPage(pageNumber);
            if (page == null) return; // out of bounds

            var remaining = count;
            while (remaining > 0)
            {
                var chunkEnd = Page.PageDataCapacity;
                var chunkLength = Math.Min(chunkEnd - chunkOffset, remaining);

                if (chunkLength > 0)
                {
                    page.Write(buffer, offset, chunkOffset, chunkLength);
                    _parent.CommitPage(page);

                    remaining -= chunkLength;
                    offset += chunkLength;
                    _requestedOffset += chunkLength;
                }

                if (remaining == 0) break;

                chunkOffset = 0;

                // extend page chain
                var next = _parent.WalkPageChain(page);
                if (next == null) next = _parent.ChainPage(page, null, -1);
                page = next;
            }
            _mostRecentPage = page;
        }
        /// <inheritdoc />
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => _enableWriting;

        /// <inheritdoc />
        public override long Length
        {
            get
            {
                return (_endPage.DocumentSequence * Page.PageDataCapacity) + _endPage.PageDataLength;
            }
        }

        /// <inheritdoc />
        public override long Position { get { return _requestedOffset; } set { Seek(value, SeekOrigin.Begin); } }
    }
}