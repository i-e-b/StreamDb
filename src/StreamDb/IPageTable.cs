using System;
using System.Collections.Generic;
using System.IO;
using JetBrains.Annotations;

namespace StreamDb
{
    public interface IPageTable
    {
        // ############## Write ##############
        
        /// <summary>
        /// Write a new document to data pages and the index.
        /// Returns new document ID.
        /// </summary>
        /// <param name="data">Stream to use as document source. It will be read from current position to end.</param>
        Guid WriteDocument(Stream data);

        /// <summary>
        /// Bind a document ID to a path. If there was an existing document in that path,
        /// its ID will be returned.
        /// </summary>
        Guid BindPathToDocument(string path, Guid id);

        // ############## Delete ##############

        /// <summary>
        /// Delete a document page chain. Does NOT directly affect the path index or document index
        /// </summary>
        void DeleteDocument(Guid oldId);

        /// <summary>
        /// Delete a document page chain. Does NOT directly affect the path index or document index
        /// </summary>
        void DeleteSinglePathForDocument(Guid documentId, string path);

        /// <summary>
        /// Remove a document from the main index.
        /// You should also call `DeletePathsForDocument` and `DeleteDocument`
        /// </summary>
        /// <remarks>
        /// This really just marks the document as invalid. We might add some garbage collection later.
        /// </remarks>
        void RemoveFromIndex(Guid id);

        /// <summary>
        /// Unbind all paths for the given document ID.
        /// This does not delete the document page chain or update the document index
        /// </summary>
        void DeletePathsForDocument(Guid id);
        
        // ############## Read ##############

        /// <summary>
        /// Try to find a document ID for a given path.
        /// Returns empty guid if not found.
        /// There is no guarantee that the document will still be present in the page table. You will need to do a subsequent read.
        /// </summary>
        Guid GetDocumentIdByPath(string path);

        /// <summary>
        /// Return all paths bound to a document that share a path prefix
        /// </summary>
        [NotNull]IEnumerable<string> SearchPaths(string pathPrefix);

        /// <summary>
        /// List all paths that match a document id
        /// </summary>
        [NotNull]IEnumerable<string> ListPathsForDocument(Guid documentId);

        /// <summary>
        /// Present a stream to read from a document, recovered by ID.
        /// Returns null if the document is not found.
        /// </summary>
        Stream ReadDocument(Guid id);

        // ############## Info ##############
        
        /// <summary>
        /// Scan the free page chain, count how many slots are occupied
        /// </summary>
        int CountFreePages();
    }
}