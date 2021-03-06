﻿using System;
using System.Collections.Generic;
using System.IO;
using JetBrains.Annotations;

namespace StreamDb.Internal.Core
{
    /// <summary>
    /// A db implementation that uses `PageStreamStorage` as the back-end
    /// </summary>
    internal class PageStorageBackend : IDatabaseBackend
    {
        [NotNull]private readonly PageStorage _core;

        public PageStorageBackend(Stream fs) {
            if (fs == null) throw new Exception("Storage stream must not be null");
            _core = new PageStorage(fs);
        }

        /// <inheritdoc />
        public Guid WriteDocument(Stream data)
        {
            var pageHead = _core.WriteStream(data);
            var docId = Guid.NewGuid();
            _core.BindIndex(docId, pageHead, out _);
            return docId;
        }

        /// <inheritdoc />
        public Guid BindPathToDocument(string path, Guid id)
        {
            _core.BindPath(path, id, out var prev);
            return prev ?? Guid.Empty;
        }

        /// <inheritdoc />
        public void DeleteDocument(Guid oldId) {
            var all = _core.GetPathsForDocument(oldId);
            foreach (var path in all)
            {
                _core.UnbindPath(path);
            }
            var pageId = _core.GetDocumentHead(oldId);
            _core.UnbindIndex(oldId);
            _core.ReleaseChain(pageId);
        }

        /// <inheritdoc />
        public void DeleteSinglePathForDocument(Guid documentId, string path) {
            _core.UnbindPath(path);
        }

        /// <inheritdoc />
        public void RemoveFromIndex(Guid id) {
            _core.UnbindIndex(id);
        }

        /// <inheritdoc />
        public void DeletePathsForDocument(Guid id) {
            var all = _core.GetPathsForDocument(id);
            foreach (var path in all)
            {
                _core.UnbindPath(path);
            }
        }

        /// <inheritdoc />
        public Guid GetDocumentIdByPath(string path) { 
            return _core.GetDocumentIdByPath(path) ?? Guid.Empty;
        }

        /// <inheritdoc />
        public IEnumerable<string> SearchPaths(string pathPrefix) {
            return _core.SearchPaths(pathPrefix);
        }

        /// <inheritdoc />
        public IEnumerable<string> ListPathsForDocument(Guid documentId) { 
            return _core.GetPathsForDocument(documentId);
        }

        /// <inheritdoc />
        public Stream? ReadDocument(Guid id) {
            try
            {
                var pageHead = _core.GetDocumentHead(id);
                if (pageHead < 0) return null;
                return _core.GetStream(pageHead);
            }
            catch (Exception ex)
            {
                throw new Exception("Data integrity check failed", ex);
            }
        }
        
        /// <inheritdoc />
        public string GetInfo(Guid id) {
            try
            {
                var pageHead = _core.GetDocumentHead(id);
                if (pageHead < 0) return "Document not found";
                var page = _core.GetRawPage(pageHead);
                if (page == null) return "Failed to read document header";
                return $"{page.DataLength} bytes; file index = {page.PageId}; CRC = {page.CrcHash:X};";
            }
            catch (Exception ex)
            {
                return "Data integrity check failed: " + ex.Message;
            }
        }

        /// <inheritdoc />
        public int CountFreePages() { return 0; }
    }
}