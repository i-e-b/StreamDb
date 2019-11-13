using System;
using System.Collections.Generic;
using System.IO;
using JetBrains.Annotations;

namespace StreamDb.Internal.Core
{
    /// <summary>
    /// READ-ONLY stream abstraction over a page-chain
    /// </summary>
    public class SimplePageStream : Stream
    {
        [NotNull]private readonly PageStreamStorage _parent;
        /// <summary>Mapping of (position in page chain) to (global page id)</summary>
        [NotNull]private readonly List<int> _pageIdCache;

        public SimplePageStream([NotNull]PageStreamStorage parent, int endPageId)
        {
            _parent = parent;
            _pageIdCache = new List<int>();

            Length = LoadPageIdCache(parent, endPageId);
        }

        private long LoadPageIdCache([NotNull]PageStreamStorage parent, int endPageId)
        {
            long length = 0;
            var s = new Stack<int>();
            var p = parent.GetRawPage(endPageId);
            while (p != null)
            {
                s.Push(p.PageId);
                length += p.DataLength;
                p = parent.GetRawPage(p.PrevPageId);
            }

            while (s.Count > 0) _pageIdCache.Add(s.Pop());
            return length;
        }

        /// <inheritdoc />
        public override void Flush() { }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            // We assume all pages are filled except the last one.
            var pageIdx = (int) (Position / SimplePage.PageDataCapacity);
            var startingOffset = (int) (Position % SimplePage.PageDataCapacity);

            if (pageIdx < 0 || pageIdx >= _pageIdCache.Count) throw new Exception("Read started out of the bounds of page chain");

            var remains = (int)Math.Min(count, Length - Position);
            var written = 0;

            while (remains > 0) {
                var page = _parent.GetRawPage(_pageIdCache[pageIdx]);
                if (page == null) throw new Exception($"Page {_pageIdCache[pageIdx]} lost between cache and read");
                var available = (int) (page.DataLength - startingOffset);
                if (available < 1) throw new Exception($"Read from page chain returned nonsense bytes available ({available})");
                var stream = page.Freeze();
                stream.Seek(startingOffset + SimplePage.PageHeadersSize, SeekOrigin.Begin);

                stream.Read(buffer, written+offset, available);
                written += available;
                remains -= available;

                pageIdx++;
                startingOffset = 0;
            }

            return written;
        }

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Position = offset;
                    return Position;

                case SeekOrigin.Current:
                    Position = Math.Min(Position + offset, Length);
                    return Position;

                case SeekOrigin.End:
                    Position = Length + offset;
                    return Position;

                default: throw new Exception("Non exhaustive switch");
            }
        }

        /// <inheritdoc />
        public override void SetLength(long value) { throw new InvalidOperationException("Page stream is not writable"); }

        /// <inheritdoc />
        public override void Write(byte[] buffer, int offset, int count) { throw new InvalidOperationException("Page stream is not writable"); }

        /// <inheritdoc />
        public override bool CanRead => true;
        /// <inheritdoc />
        public override bool CanSeek => true;

        /// <inheritdoc />
        public override bool CanWrite => false;

        /// <inheritdoc />
        public override long Length { get; }

        /// <inheritdoc />
        public override long Position { get; set; }
    }
}