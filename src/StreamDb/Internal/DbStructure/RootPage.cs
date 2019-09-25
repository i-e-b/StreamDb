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
    public class RootPage:IByteSerialisable
    {
        [NotNull]private readonly VersionedLink _indexLink;
        [NotNull]private readonly VersionedLink _freeListLink;
        [NotNull]private readonly VersionedLink _pathLookupLink;

        public RootPage()
        {
            _indexLink = new VersionedLink();
            _freeListLink = new VersionedLink();
            _pathLookupLink = new VersionedLink();
        }


        /// <inheritdoc />
        public void FromBytes(byte[] source)
        {
            if (source == null) throw new Exception("IndexPage.FromBytes: data was too short.");
            using (var ms = new MemoryStream(source))
            {
                ms.Seek(0, SeekOrigin.Begin);
                var r = new BinaryReader(ms);

                _freeListLink.FromBytes(r.ReadBytes(VersionedLink.ByteSize));
                _indexLink.FromBytes(r.ReadBytes(VersionedLink.ByteSize));
                _pathLookupLink.FromBytes(r.ReadBytes(VersionedLink.ByteSize));
            }
        }
        
        /// <inheritdoc />
        public byte[] ToBytes()
        {
            using (var ms = new MemoryStream())
            {
                var w = new BinaryWriter(ms);

                w.Write(_freeListLink.ToBytes());
                w.Write(_indexLink.ToBytes());
                w.Write(_pathLookupLink.ToBytes());

                ms.Seek(0, SeekOrigin.Begin);
                return ms.ToArray() ?? throw new Exception();
            }
        }

        /// <summary>
        /// Add a new pointer to the first page of the DocumentID index page list.
        /// This may replace earlier versions
        /// </summary>
        public void AddIndex(int pageId, out int expired) { _indexLink.WriteNewLink(pageId, out expired); }

        /// <summary>
        /// Add a new pointer to the first page of the PageId free page list.
        /// This may replace earlier versions
        /// </summary>
        public void AddFreeList(int pageId, out int expired){ _freeListLink.WriteNewLink(pageId, out expired); }

        /// <summary>
        /// Add a new pointer to the first page of the PageId free page list.
        /// This may replace earlier versions
        /// </summary>
        public void AddPathLookup(int pageId, out int expired){ _pathLookupLink.WriteNewLink(pageId, out expired); }

    }
}