using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.DbStructure
{
    /// <summary>
    /// Data structure for the root page. This is a versioned list of the starting points for the index/free tables.
    /// This page should be updated very rarely.
    /// </summary>
    public class RootPage:IStreamSerialisable
    {
        [NotNull]public readonly VersionedLink IndexLink;
        [NotNull]public readonly VersionedLink FreeListLink;
        [NotNull]public readonly VersionedLink PathLookupLink;

        public RootPage()
        {
            IndexLink = new VersionedLink();
            FreeListLink = new VersionedLink();
            PathLookupLink = new VersionedLink();
        }


        /// <inheritdoc />
        public void FromBytes(Stream source)
        {
            if (source == null) throw new Exception("IndexPage.FromBytes: data was too short.");
            var r = new BinaryReader(source);

            FreeListLink.FromBytes(r.BaseStream);
            IndexLink.FromBytes(r.BaseStream);
            PathLookupLink.FromBytes(r.BaseStream);
        }

        /// <inheritdoc />
        public Stream ToBytes()
        {
            var ms = new MemoryStream();

            FreeListLink.ToBytes().CopyTo(ms);
            IndexLink.ToBytes().CopyTo(ms);
            PathLookupLink.ToBytes().CopyTo(ms);

            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

        /// <summary>
        /// Add a new pointer to the first page of the DocumentID index page list.
        /// This may replace earlier versions
        /// </summary>
        public void AddIndex(int pageId, out int expired) { IndexLink.WriteNewLink(pageId, out expired); }

        /// <summary>
        /// Add a new pointer to the first page of the PageId free page list.
        /// This may replace earlier versions
        /// </summary>
        public void AddFreeList(int pageId, out int expired){ FreeListLink.WriteNewLink(pageId, out expired); }

        /// <summary>
        /// Add a new pointer to the first page of the PageId free page list.
        /// This may replace earlier versions
        /// </summary>
        public void AddPathLookup(int pageId, out int expired){ PathLookupLink.WriteNewLink(pageId, out expired); }

        public int GetFreeListPageId()
        {
            if (!FreeListLink.TryGetLink(0, out var id)) return -1;
            return id;
        }

        public int GetIndexListId()
        {
            if (!IndexLink.TryGetLink(0, out var id)) return -1;
            return id;
        }

        public int GetPathLookupBase()
        {
            if (!PathLookupLink.TryGetLink(0, out var id)) return -1;
            return id;
        }
    }
}