using System;
using System.Collections.Generic;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.DbStructure;

namespace StreamDb.Internal.Core
{
    /// <summary>
    /// READ-ONLY stream abstraction over a page-chain
    /// </summary>
    public class SimplePageStream : Stream
    {
        [NotNull]private readonly PageStorage _parent;
        private readonly int _endPageId;

        /// <summary>Pages loaded from the DB</summary>
        [NotNull]private readonly List<BasicPage> _pageIdCache;

        private long _length;
        private bool _cached;

        public SimplePageStream([NotNull]PageStorage parent, int endPageId)
        {
            _cached = false;
            _parent = parent;
            _endPageId = endPageId;
            _pageIdCache = new List<BasicPage>();
        }

        private void LoadPageIdCache()
        {
            if (_cached) return;
            long length = 0;
            var s = new Stack<BasicPage>();
            var p = _parent.GetRawPage(_endPageId);
            while (p != null)
            {
                s.Push(p);
                length += p.DataLength;
                p = _parent.GetRawPage(p.PrevPageId); // we end up checking all the CRCs here
            }

            while (s.Count > 0) _pageIdCache.Add(s.Pop()); // cache in forward-order
            _length = length;
            _cached = true;
        }

        /// <inheritdoc />
        public override void Flush() { }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null) throw new Exception("Destination buffer must not be null");
            LoadPageIdCache(); // make sure data is loaded

            var pageIdx = (int) (Position / BasicPage.PageDataCapacity);
            var startingOffset = (int) (Position % BasicPage.PageDataCapacity);

            if (pageIdx < 0) throw new Exception("Read started out of the bounds of page chain");
            if (pageIdx >= _pageIdCache.Count) return 0; // ran off the end

            var remains = (int)Math.Min(count, Length - Position);
            var written = 0;

            while (remains > 0) {
                var page = _pageIdCache[pageIdx]; // ignore CRCs here, as we checked them at stream creation time
                if (page == null) throw new Exception($"Page {_pageIdCache[pageIdx]} lost between cache and read");
                var available = (int) (page.DataLength - startingOffset);
                if (available < 1) throw new Exception($"Read from page chain returned nonsense bytes available ({available})");

                var stream = page.BodyStream();
                stream.Seek(startingOffset, SeekOrigin.Begin);

                var request = Math.Min(available, count - written);
                if (request < 1) throw new Exception("Read stalled");
                if (request + written + offset > buffer.Length) throw new Exception($"Would overrun buffer ({request}+{written}+{offset} > {buffer.Length})");

                var actual = stream.Read(buffer, written + offset, request);
                if (actual < 1) throw new Exception("Stream read did not progress");
                written += actual;
                remains -= actual;

                pageIdx++;
                startingOffset = 0;
            }
            
            Position += written;
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
        public override long Length { get { LoadPageIdCache(); return _length; } }

        /// <inheritdoc />
        public override long Position { get; set; }
    }
}