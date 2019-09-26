using System;
using System.IO;
using JetBrains.Annotations;

namespace StreamDb.Internal.DbStructure
{
    public class PageTableStream : Stream {
        [NotNull]private readonly PageTable _parent;
        [NotNull]private readonly Page _endPage;
        private long _requestedOffset;

        /// <summary>
        /// Wrap a page chain in a stream reader.
        /// </summary>
        /// <param name="parent">PageTable to be used for traversal and reading</param>
        /// <param name="endPage">The LAST page of the page chain</param>
        public PageTableStream([NotNull]PageTable parent, Page endPage)
        {
            _requestedOffset = 0;
            _parent = parent;
            _endPage = endPage ?? throw new Exception("Tried to stream from a null page");
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


            var page = _parent.FindPageInChain(_endPage, pageNumber);
            if (page == null) return 0; // out of bounds

            var remaining = count;
            var read = 0;
            while (remaining > 0)
            {
                var data = page.GetData();

                var chunkEnd = Math.Min(Page.PageDataCapacity, chunkOffset + remaining);
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
            return read;
        }

        /// <summary>
        /// Seek to a specific position for next read. Seek from end is not supported.
        /// </summary>
        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin) {
                case SeekOrigin.Begin:
                    _requestedOffset = offset;
                    return _requestedOffset;

                case SeekOrigin.Current:
                    _requestedOffset+= offset;
                    return _requestedOffset;

                default: throw new Exception("Seek from end has not been implemented.");
            }
        }

        public override void SetLength(long value) { }
        public override void Write(byte[] buffer, int offset, int count) { 
            throw new Exception("Page content streams are read-only");
        }
        /// <inheritdoc />
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;

        /// <inheritdoc />
        public override long Length
        {
            get
            {
                // TODO: subtract some from the last page based on doc size
                return _endPage.DocumentSequence * Page.PageDataCapacity;
            }
        }

        /// <inheritdoc />
        public override long Position { get { return _requestedOffset; } set { Seek(value, SeekOrigin.Begin); } }
    }
}