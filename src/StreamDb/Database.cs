﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using JetBrains.Annotations;
using StreamDb.Internal.Core;
using StreamDb.Internal.DbStructure;

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
        [NotNull]   private readonly IDatabaseBackend    _pages;

        private Database(Stream fs)
        {
            _fs = fs ?? throw new ArgumentNullException(nameof(fs));
            // ####### HERE #########
            // Is where we pick the underlying engine.
            _pages = new PageStorageBackend(_fs);
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

        /// <summary>
        /// Flush, close and dispose of the underlying stream.
        /// </summary>
        public void Dispose() { Flush(); _fs.Dispose(); }

        [NotNull]private readonly object _pathWriteLock = new object();

        /// <summary>
        /// Write a document to the given path. If an existing document uses this path, it will be deleted.
        /// </summary>
        /// <param name="path">Path that can be used with `Get` and `Search` operations to recover this document</param>
        /// <param name="data">Stream containing document data. It will be read from current position to end.</param>
        public Guid WriteDocument(string path, Stream? data)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            var id = _pages.WriteDocument(data);
            if (id == Guid.Empty) throw new Exception("Failed to write document data");

            var oldId = _pages.BindPathToDocument(path, id);

            if (oldId != Guid.Empty && oldId != id)
            {
                var others = _pages.ListPathsForDocument(oldId).Any();
                if (!others) _pages.DeleteDocument(oldId);
            }
            return id;
        }

        /// <summary>
        /// Read a document at the given path.
        /// Returns true if found, false if not found.
        /// </summary>
        public bool Get(string path, out Stream? stream)
        {
            stream = null;

            var id = _pages.GetDocumentIdByPath(path);
            if (id == Guid.Empty) return false;

            stream = _pages.ReadDocument(id);
            return stream != null;
        }

        /// <summary>
        /// Try to look up the document ID bound to a path.
        /// </summary>
        public bool GetIdByPath(string path, out Guid id) {
            id = _pages.GetDocumentIdByPath(path);
            return id != Guid.Empty;
        }
        

        /// <summary>
        /// Get a summary of the document at a given path
        /// </summary>
        public string GetDocumentInfo(string path)
        {
            var id = _pages.GetDocumentIdByPath(path);
            return _pages.GetInfo(id);
        }

        /// <summary>
        /// Add a new path binding to a document ID.
        /// If the path is already bound to a document, the old document ID will be returned
        /// </summary>
        /// <param name="documentId">ID of an existing document (this is not checked)</param>
        /// <param name="newPath">path that can be used for `Get` and `Search` operations</param>
        public Guid BindToPath(Guid documentId, string newPath)
        {
            lock (_pathWriteLock)
            {
                return _pages.BindPathToDocument(newPath, documentId);
            }
        }

        /// <summary>
        /// For a given document ID, find all paths that are bound to it.
        /// </summary>
        /// <param name="documentId">A document stored in the database</param>
        /// <returns>Enumeration of paths. This may not be multi-enumerable</returns>
        [NotNull, ItemNotNull]
        public IEnumerable<string> ListPaths(Guid documentId)
        {
            return _pages.ListPathsForDocument(documentId);
        }

        /// <summary>
        /// Delete a document from the database, and unbind all paths to it.
        /// If the document does not exist, the request will be silently ignored.
        /// </summary>
        /// <param name="documentId">Id of the document to delete.</param>
        public void Delete(Guid documentId)
        {
            _pages.DeletePathsForDocument(documentId);
            _pages.RemoveFromIndex(documentId);
            _pages.DeleteDocument(documentId);
        }
        
        /// <summary>
        /// Delete a document from the database, and unbind all paths to it.
        /// If the document does not exist, the request will be silently ignored.
        /// </summary>
        /// <param name="path">Any path that the document is bound to</param>
        public void Delete(string path)
        {
            var id = _pages.GetDocumentIdByPath(path);
            _pages.DeletePathsForDocument(id);
            _pages.RemoveFromIndex(id);
            _pages.DeleteDocument(id);
        }

        /// <summary>
        /// Remove a single path binding for a document.
        /// If the path is not currently bound to that document, the request will be silently ignored
        /// </summary>
        /// <param name="documentId">Id of document currently bound to the path</param>
        /// <param name="path">Path to unbind</param>
        public void UnbindPath(Guid documentId, string path)
        {
            _pages.DeleteSinglePathForDocument(documentId, path);
        }

        /// <summary>
        /// Given the start of a path string, returns all matching paths that have a document bound to them
        /// </summary>
        /// <param name="pathPrefix">Start of a path string</param>
        [NotNull, ItemNotNull]
        public IEnumerable<string> Search(string pathPrefix)
        {
            return _pages.SearchPaths(pathPrefix);
        }

        /// <summary>
        /// Scan the database for statistics.
        /// </summary>
        /// <param name="totalPages">The number of pages in storage (based on storage size)</param>
        /// <param name="freePages">The number of free pages that can be written without increasing storage</param>
        public void CalculateStatistics(out int totalPages, out int freePages)
        {
            totalPages = (int) (_fs.Length / BasicPage.PageRawSize);
            freePages = _pages.CountFreePages();
        }

        /// <summary>
        /// Attempt to synchronously flush the underlying storage
        /// </summary>
        /// <remarks>The default file flush is essentially a no-op,
        /// so we try to detect that case and force a to-disk flush.</remarks>
        public void Flush()
        {
            if (_fs is FileStream file) {
                file.Flush(true);
            } else {
                _fs.Flush();
            }
        }

        /// <summary>
        /// If you call this method, CRC checks will be ignored on READ (still calculated for WRITE).
        /// This makes read-heavy workloads about 10x faster, but data corruption will go unreported. 
        /// </summary>
        public static void SetQuickAndDirtyMode() {
            BasicPage.QuickAndDirtyMode = true;
        }
    }
}