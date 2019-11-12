using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.Core;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.DbStructure
{

    /// <summary>
    /// Represents a generalised page in the DB with known contents
    /// </summary>
    public class Page<T> : ComplexPage where T : IStreamSerialisable, new()
    {
        /// <summary>
        /// Snapshot of the page content when it was loaded
        /// </summary>
        [NotNull]public T View { get; set; }

        /// <summary>
        /// Read a single page and take a snapshot of it's contents
        /// </summary>
        /// <param name="pageId">pageID that has been loaded</param>
        /// <param name="bytes">page data</param>
        public Page(int pageId, Stream bytes)
        {
            Defrost(bytes);

            if (!ValidateCrc()) throw new Exception($"Data integrity check failed loading page {pageId}");

            OriginalPageId = pageId;

            var v = new T();
            v.Defrost(GetDataStream());
            View = v;
        }

        /// <summary>
        /// Wrap a raw page with a view model
        /// </summary>
        [NotNull]public static Page<T> FromRaw(ComplexPage rawPage) {
            if (rawPage == null) throw new ArgumentNullException(nameof(rawPage));
            return new Page<T>(rawPage.OriginalPageId, new MemoryStream(rawPage._data));
        }

        /// <summary>
        /// Write the View's data back into the raw page
        /// </summary>
        public void SyncView()
        {
            var bytes = View.Freeze();
            Write(bytes, 0, 0, bytes.Length);
            //UpdateCRC();
        }
    }

}