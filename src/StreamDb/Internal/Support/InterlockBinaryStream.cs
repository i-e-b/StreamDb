using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace StreamDb.Internal.Support
{
    /// <summary>
    /// Provides thread-locked access to a stream, as either a BinaryReader or BinaryWriter
    /// </summary>
    internal class InterlockBinaryStream : IDisposable
    {
        private volatile Stream _token, _master;
        private readonly bool _closeBase;

        public InterlockBinaryStream([NotNull]Stream baseStream, bool closeBaseStream = true)
        {
            _token = baseStream;
            _master = baseStream;
            _closeBase = closeBaseStream;
        }

        /// <summary>
        /// Wait for access to the reader.
        /// *MUST* always be released correctly
        /// </summary>
        [NotNull]
        public BinaryReader AcquireReader()
        {
            var stream = Interlocked.Exchange(ref _token, null);
            while (stream == null)
            {
                Task.Delay(1)?.Wait();
                stream = Interlocked.Exchange(ref _token, null);
            }
            return new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
        }

        /// <summary>
        /// Wait for access to the writer.
        /// *MUST* always be released correctly
        /// </summary>
        [NotNull]
        public BinaryWriter AcquireWriter()
        {
            var stream = Interlocked.Exchange(ref _token, null);
            while (stream == null)
            {
                Task.Delay(1)?.Wait();
                stream = Interlocked.Exchange(ref _token, null);
            }
            return new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);
        }

        public void Release(ref BinaryReader reader)
        {
            if (reader == null || reader.BaseStream != _master) throw new Exception("Invalid threadlock stream release (reader)");
            reader.Dispose();
            var old = Interlocked.Exchange(ref _token, _master);
            if (old != null) throw new Exception("Invalid interlock");
            reader = null;
        }

        public void Release(ref BinaryWriter writer)
        {
            if (writer == null || writer.BaseStream != _master) throw new Exception("Invalid threadlock stream release (writer)");
            writer.Flush();
            writer.Dispose();
            var old = Interlocked.Exchange(ref _token, _master);
            if (old != null) throw new Exception("Invalid interlock");
            writer = null;
        }

        public void Close()
        {
            _token = null;
            if (_closeBase) _master?.Dispose();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Close();
        }

        public void Flush()
        {
            _master?.Flush();
        }
    }
}