using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.DbStructure;
using StreamDb.Internal.Support;

namespace StreamDb
{
    /// <summary>
    /// This is the entry point to the data storage. Access the database through the methods here.
    /// </summary>
    /// <remarks>
    /// The idea behind the DB is:
    /// 1. All documents have a single unique DocID (guid). This is assigned by the DB engine.
    /// 2. Each document may be connected to as many 'paths' as needed. These are arbitrary strings.
    ///
    /// The database is optimised for many more reads than writes, and rare deletes.
    /// The upper limit of individual document size is determined by internal counters. Currently this is 256 MB.
    /// The overall database storage limit is determined by pageID limit (2147483647) times page data capacity (4061 bytes); this is 8000 GB
    ///
    /// The database is designed to allow for rapid connect/disconnect cycles to support multiple access.
    /// It should also be 100% thread safe within a single process.
    /// </remarks>
    public class Database : IDisposable
    {
        [NotNull]   private readonly Stream       _fs;
        [NotNull]   private readonly PageTable    _pages;

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
        public void Dispose() { _fs.Flush(); _fs.Dispose(); }

        /// <summary>
        /// Write a document to the given path
        /// </summary>
        public void WriteDocument(string path, Stream data)
        {
            var id = _pages.WriteDocument(data);
            if (id == Guid.Empty) throw new Exception("Failed to write document data");
            var oldId = _pages.BindPathToDocument(path, id);

            if (oldId != Guid.Empty && oldId != id) {
                // replaced an existing document
                _pages.DeleteDocument(oldId);
            }
        }

        /// <summary>
        /// Read a document at the given path.
        /// Returns true if found, false if not found.
        /// </summary>
        public bool Get(string path, out Stream stream)
        {
            stream = null;

            var id = _pages.GetDocumentIdByPath(path);
            if (id == Guid.Empty) return false;

            stream = _pages.ReadDocument(id);
            return stream != null;
        }
    }
}