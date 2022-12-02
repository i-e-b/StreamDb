using System;
using System.IO;
using JetBrains.Annotations;

namespace StreamDb.Internal.Support
{
    /// <summary>
    /// Lightweight wrapper around a stream section.
    /// This will read through the parent stream, but stop after a given length.
    /// The parent stream is not copied, and its position will be updated
    /// </summary>
    public class Substream : Stream
    {
        [NotNull]private readonly Stream _parent;
        private readonly long _length;
        private readonly long _endPosition;
        private readonly long _startPosition;

        /// <summary>
        /// Create a new substream that starts at the current position, and ends after the given length in bytes
        /// </summary>
        /// <param name="s">parent stream</param>
        /// <param name="length">length of substream</param>
        public Substream(Stream s, int length)
        {
            if (s == null) throw new Exception("Tried to subrange a null stream");
            if (!s.CanRead) throw new Exception("Parent stream must be readable");
            _parent = s;
            _startPosition = s.Position;
            _length = length;
            _endPosition = Math.Min(length + s.Position, s.Length);
            if (_startPosition > s.Length || _startPosition > _endPosition) {
                throw new Exception($"Invalid substream created. Starts at {_startPosition}, ends at {_endPosition}; Source length = {s.Length}");
            }
        }

        /// <inheritdoc />
        public override void Flush() { _parent.Flush(); }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            var modCount = Math.Min(_endPosition - Position, count);
            return _parent.Read(buffer, offset, checked((int)modCount));
        }

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin)
        {
            var target = _startPosition;
            switch (origin)
            {
                case SeekOrigin.Begin:
                    target += offset;
                    break;

                case SeekOrigin.Current:
                    target = Position + offset;
                    break;

                case SeekOrigin.End:
                    target = _endPosition + offset;
                    break;
            }
            if (target > _endPosition) target = _endPosition;
            if (target < _startPosition) target = _startPosition;
            return _parent.Seek(target, SeekOrigin.Begin);
        }

        /// <inheritdoc />
        public override void SetLength(long value) => throw new Exception("Can't set the length of a substream");

        /// <inheritdoc />
        public override void Write(byte[] buffer, int offset, int count)
        {
            var modCount = Math.Min(_endPosition - Position, count);
            _parent.Write(buffer, offset, (int) modCount);
        }

        /// <inheritdoc />
        public override bool CanRead => true;

        /// <inheritdoc />
        public override bool CanSeek => true;

        /// <inheritdoc />
        public override bool CanWrite => false;

        /// <inheritdoc />
        // ReSharper disable once ConvertToAutoProperty
        public override long Length => _length;

        /// <inheritdoc />
        public override long Position
        {
            get { return _parent.Position - _startPosition; }
            set { _parent.Position = value + _startPosition; }
        }

        /// <summary>
        /// Size of data that can be read
        /// </summary>
        public long AvailableData()
        {
            return _endPosition - _startPosition;
        }
    }
}