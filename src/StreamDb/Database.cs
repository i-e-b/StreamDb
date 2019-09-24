using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal;
using StreamDb.Internal.DbStructure;
using StreamDb.Internal.Support;

namespace StreamDb
{
    /// <summary>
    /// This is the entry point to the data storage
    /// </summary>
    public class Database : IDisposable
    {
        [NotNull]   private readonly Stream       _fs;
        [NotNull]   private readonly PageTable    _pages;
        [CanBeNull] private PathIndex<SerialGuid> _pathIndexCache;

        private Database(Stream fs)
        {
            _fs = fs ?? throw new ArgumentNullException(nameof(fs));
            _pages = new PageTable(_fs);
        }

        /// <summary>
        /// Open a connection to a datastore by seekable stream.
        /// Throws an exception if the stream does not support seeking and reading.
        /// <para></para>
        /// If an empty stream is provided (length == 0), it will be initialised. Otherwise it must be
        /// a valid storage stream.
        /// </summary>
        public static Database TryConnect(Stream storage)
        {
            if (storage == null || !storage.CanSeek || !storage.CanRead) throw new ArgumentException("Storage stream must support seeking and reading", nameof(storage));

            if (storage.Length == 0)
            {
                if (!storage.CanWrite) throw new ArgumentException("Attempted to initialise a read-only stream", nameof(storage));
                storage.Seek(0, SeekOrigin.Begin);
            }

            return new Database(storage);
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _fs.Dispose();
        }
    }
}