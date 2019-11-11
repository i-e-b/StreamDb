using System;
using System.IO;
using JetBrains.Annotations;

namespace StreamDb.Internal.Support
{
    /// <summary>
    /// A pair of versioned page links, and calls to read and update them
    /// </summary>
    public class VersionedLink : IStreamSerialisable {
        [NotNull] private PageLink _linkA;
        [NotNull] private PageLink _linkB;

        public VersionedLink()
        {
            _linkA = PageLink.InvalidLink();
            _linkB = PageLink.InvalidLink();
        }

        public const int ByteSize = 10;

        // Clumsy lock. Need to integrate read/write and update to improve
        [NotNull]private readonly object _lock = new object();

        /// <summary>
        /// Try to get a link version. Returns false if the version is not set.
        /// </summary>
        /// <param name="revision">0 is the newest link, 1 is the next oldest</param>
        /// <param name="pageId">The page ID of this version, or -1 if none are valid</param>
        /// <returns>true if a link is available, false otherwise</returns>
        public bool TryGetLink(int revision, out int pageId) {
            lock (_lock)
            {
                pageId = -1;
                if (revision > 1 || revision < 0) return false; // not supported
                if (_linkA.PageId <= 0 && _linkB.PageId <= 0) return false; // no versions

                if (_linkB.PageId < 0)
                { // B hasn't been written
                    pageId = (revision == 0) ? _linkA.PageId : _linkB.PageId;
                    return pageId > 0;
                }

                if (_linkA.Version == _linkB.Version) throw new Exception("VersionedLink.TryGetLink: option table versions invalid");

                if (_linkA.Version > _linkB.Version) // B is older
                {
                    pageId = (revision == 0) ? _linkA.PageId : _linkB.PageId;
                    return pageId > 0;
                }

                pageId = (revision == 0) ? _linkB.PageId : _linkA.PageId;
                return pageId > 0;
            }
        }

        public void WriteNewLink(int pageId, out int expiredPage) {
            lock (_lock)
            {
                expiredPage = -1;

                if (_linkA.PageId < 0)
                {
                    // A has never been set
                    _linkA = new PageLink
                    {
                        PageId = pageId,
                        Version = new MonotonicByte(0)
                    };
                    return;
                }

                if (_linkB.PageId < 0)
                {
                    // B has never been set
                    _linkB = new PageLink
                    {
                        PageId = pageId,
                        Version = _linkA.Version.GetNext()
                    };
                    return;
                }

                if (_linkA.Version == _linkB.Version) throw new Exception("VersionedLink.WriteNewLink: option table versions invalid");

                if (_linkA.Version > _linkB.Version)
                {
                    // B is older. Replace it.
                    expiredPage = _linkB.PageId;
                    _linkB = new PageLink
                    {
                        PageId = pageId,
                        Version = _linkA.Version.GetNext()
                    };
                    return;
                }

                // A is older. Replace it.
                expiredPage = _linkA.PageId;
                _linkA = new PageLink
                {
                    PageId = pageId,
                    Version = _linkB.Version.GetNext()
                };
            }
        }

        private void WriteLink([NotNull]BinaryWriter w, PageLink link)
        {
            if (link != null)
            {
                w.Write((byte)link.Version.Value);
                w.Write(link.PageId);
            }
            else
            {
                w.Write((byte)0);
                w.Write(-1);
            }
        }

        /// <inheritdoc />
        public Stream Freeze()
        {
            lock (_lock)
            {
                var ms = new MemoryStream(ByteSize);
                var w = new BinaryWriter(ms);
                WriteLink(w, _linkA);
                WriteLink(w, _linkB);

                ms.Seek(0, SeekOrigin.Begin);
                return ms;
            }
        }

        /// <inheritdoc />
        public void Defrost(Stream source)
        {
            lock (_lock)
            {
                if (source == null || source.Length < ByteSize) throw new Exception("VersionedLink.FromBytes: data was too short.");
                var r = new BinaryReader(source);
                _linkA = new PageLink
                {
                    Version = new MonotonicByte(r.ReadByte()),
                    PageId = r.ReadInt32()
                };
                _linkB = new PageLink
                {
                    Version = new MonotonicByte(r.ReadByte()),
                    PageId = r.ReadInt32()
                };
            }
        }
    }
}