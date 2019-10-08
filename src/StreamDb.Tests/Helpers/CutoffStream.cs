using System;
using System.IO;

namespace StreamDb.Tests.Helpers
{
    public class CutoffStream: Stream {
        private readonly Stream _streamToWrap;
        private int _cutoff;

        public CutoffStream(Stream streamToWrap)
        {
            _streamToWrap = streamToWrap;
            _cutoff = int.MaxValue;
        }

        /// <summary>
        /// Set the stream to go read-only after the given number of written bytes
        /// </summary>
        public void CutoffAfter(int bytes) {
            _cutoff = bytes;
        }

        /// <summary>
        /// Returns true if the stream wrapper has passed its cutoff point
        /// </summary>
        public bool HasCutoff()
        {
            return _cutoff <= 0;
        }

        /// <inheritdoc />
        public override void Flush()
        {
            _streamToWrap?.Flush();
        }

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin)
        {
            return _streamToWrap?.Seek(offset, origin) ?? -1;
        }

        /// <inheritdoc />
        public override void SetLength(long value)
        {
            _streamToWrap?.SetLength(value);
        }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            return _streamToWrap?.Read(buffer, offset, count) ?? -1;
        }

        /// <inheritdoc />
        public override void Write(byte[] buffer, int offset, int count)
        {
            if (_cutoff <= 0) return; // tripped already

            if (_cutoff <= count) {
                // write less than the whole block
                _streamToWrap?.Write(buffer, offset, _cutoff);
                _cutoff = 0;
                return;
            }

            _streamToWrap?.Write(buffer, offset, count);
            _cutoff -= count;
        }

        /// <inheritdoc />
        public override bool CanRead => _streamToWrap?.CanRead ?? false;

        /// <inheritdoc />
        public override bool CanSeek => _streamToWrap?.CanSeek ?? false;

        /// <inheritdoc />
        public override bool CanWrite => _streamToWrap?.CanWrite ?? false;

        /// <inheritdoc />
        public override long Length => _streamToWrap?.Length ?? 0;

        /// <inheritdoc />
        public override long Position {
            get => _streamToWrap?.Position ?? 0;
            set
            {
                if (_streamToWrap != null) _streamToWrap.Position = value;
            }
        }

    }
}