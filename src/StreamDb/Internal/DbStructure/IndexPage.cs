using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.DbStructure
{
    /// <summary>
    /// Content of a single index page
    /// </summary>
    public class IndexPage : IByteSerialisable
    {
        /// <summary>
        /// A versioned link to a page chain.
        /// These should always be used in pairs. The most recent is read,
        /// the older is overwritten
        /// </summary>
        /// <remarks>9 bytes</remarks>
        public class PageLink {
            /// <summary>
            /// Version of this link. Always use the latest link whose page has a valid CRC
            /// </summary>
            public MonotonicByte Version { get; set; }

            /// <summary>
            /// End of the page chain (for writing).
            /// That page will have a link back to the start (for reading)
            /// </summary>
            public int LastPage { get; set; }

            /// <summary>
            /// Return a link that is disabled
            /// </summary>
            public static PageLink InvalidLink()
            {
                return new PageLink { Version = new MonotonicByte(), LastPage = -1 };
            }
        }

        const int EntryCount = 126; // 2+4+8+16+32+64
        const int PackedSize = 3276; // (16+5+5) * 126
        
        /// <summary> This is the implicit root index. It is not allowed as a real document ID </summary>
        public static readonly Guid NeutralDocId = new Guid(new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127 });
        /// <summary> This is an ID that means 'no document'. It is not allowed as a real document ID. </summary>
        public static readonly Guid ZeroDocId = Guid.Empty;


        [NotNull] private readonly PageLink[] _linkA;
        [NotNull] private readonly PageLink[] _linkB;
        [NotNull] private readonly Guid[] _docIds;

        /*

            Layout: [ Doc Guid (16 bytes) | PageLink[0] (5 bytes) | PageLink[1] (5 bytes) ] --> 26 bytes
            We can fit 157 in a 4k page. Gives us 6 ranks (126 entries) -> 3276 bytes
            Our pages are currently 4061 bytes, so we have plenty of spare space if we can find useful metadata to store.

            We assume but don't store a root page with guid {127,127...,127}. The first two entries are 'left' and 'right' on the second level.

        */

        public IndexPage()
        {
            _linkA = new PageLink[EntryCount];
            _linkB = new PageLink[EntryCount];
            _docIds = new Guid[EntryCount];
        }

        const int SAME =  0;
        const int LESS =  -1;
        const int GREATER =  1;

        /// <summary>
        /// Try to add a new link to the index.
        /// Returns true if written, false if the index page has no space for this entry.
        /// If the document already exists, an exception will be thrown.
        /// </summary>
        /// <param name="docId">Unique ID of the document to be inserted</param>
        /// <param name="pageId">PageID of the LAST page in the document's chain.</param>
        /// <returns>True if written, false if not</returns>
        public bool TryInsert(Guid docId, int pageId)
        {
            var index = Find(docId);
            if (index < 0 || index >= EntryCount) return false; // no space

            if (_docIds[index] != ZeroDocId) throw new Exception("Tried to insert a duplicate document ID");

            // found a space. Stick it in.
            _linkA[index] = new PageLink
            {
                LastPage = pageId,
                Version = new MonotonicByte() // start at zero
            };
            _linkB[index] = PageLink.InvalidLink();
            _docIds[index] = docId;
            return true;

        }

        /// <summary>
        /// Try to find a link in this index page. Returns true if found, false if not found.
        /// If found, this will return up to two page options. Use the newest one with a valid CRC in the page.
        /// </summary>
        /// <param name="docId">Document ID to find</param>
        /// <param name="optionA">If found, this is one of the page link options. May be null</param>
        /// <param name="optionB">If found, this is one of the page link options. May be null</param>
        public bool Search(Guid docId, out PageLink optionA, out PageLink optionB) {
            optionA = null;
            optionB = null;

            var index = Find(docId);
            if (index < 0 || index >= EntryCount) return false; // not found
            if (_docIds[index] == ZeroDocId) return false; // not found
            if (_docIds[index] != docId) throw new Exception("IndexPage.Search: Logic error");

            optionA = _linkA[index];
            optionB = _linkB[index];

            return true;
        }

        /// <summary>
        /// Update a link with a new PageID. The oldest link will be updated.
        /// Returns true if a change was made. False if the link was not found in this index page
        /// </summary>
        /// <param name="docId">ID of document to update</param>
        /// <param name="pageId">PageID of the LAST page in the new document chain to be inserted</param>
        /// <param name="expiredPage">If an old value is lost, this is PageID. Otherwise -1</param>
        /// <remarks>If an existing chain is de-linked by this, all the pages should be added to the free list</remarks>
        public bool Update(Guid docId, int pageId, out int expiredPage) {
            expiredPage = -1;

            // find the entry to update
            var index = Find(docId);
            if (index < 0 || index >= EntryCount) return false; // not found
            if (_docIds[index] == ZeroDocId) return false; // not found
            if (_docIds[index] != docId) throw new Exception("IndexPage.Search: Logic error");

            var optionA = _linkA[index];
            var optionB = _linkB[index];

            if (optionA?.Version == null || optionB?.Version == null) throw new Exception("IndexPage.Update: invalid option table");

            if (optionB.LastPage < 0) {
                // B has never been set
                _linkB[index] = new PageLink
                {
                    LastPage = pageId,
                    Version = optionA.Version.GetNext()
                };
                return true;
            }

            if (optionA.Version == optionB.Version) throw new Exception("IndexPage.Update: option table versions invalid");

            if (optionA.Version > optionB.Version) {
                // B is older. Replace it.
                expiredPage = optionB.LastPage;
                _linkB[index] = new PageLink
                {
                    LastPage = pageId,
                    Version = optionA.Version.GetNext()
                };
                return true;
            }

            // A is older. Replace it.
            expiredPage = optionA.LastPage;
            _linkA[index] = new PageLink
            {
                LastPage = pageId,
                Version = optionB.Version.GetNext()
            };
            return true;

        }

        /// <summary>
        /// Find tries to find an entry index by a guid key. This is used in insert, search, update.
        /// If no such entry exists, but there is a space for it, you will get a valid index whose
        /// `_docIds` entry is Guid.Zero -- so always check.
        /// </summary>
        private int Find(Guid target) {
            // the implicit node:
            var cmpNode = NeutralDocId;
            int leftIdx = 0;
            int rightIdx = 1;

            var current = -1;

            // loop start
            for (int i = 0; i < 7; i++)
            {
                switch (cmpNode.CompareTo(target))
                {
                    case SAME: return current;

                    case LESS:
                        // move left
                        current = leftIdx;
                        break;

                    case GREATER:
                        // move right
                        current = rightIdx;
                        break;

                    default: throw new Exception("IndexTree.TryInsert: Unexpected case.");
                }

                // update next step pointers
                leftIdx = (current * 2) + 2;
                rightIdx = (current * 2) + 3;

                // check we're in bounds
                if (current < 0) throw new Exception("IndexTree.TryInsert: Logic error");
                if (current >= EntryCount) return -1;
                
                cmpNode = _docIds[current];
                if (cmpNode == ZeroDocId) { return current; } // empty space
            }
            // loop end

            throw new Exception("IndexTree.TryInsert: Out of loops bounds. Exited due to safety check.");
        }

        /// <inheritdoc />
        public void FromBytes(byte[] source)
        {
            if (source == null || source.Length < PackedSize) throw new Exception("IndexPage.FromBytes: data was too short.");
            using (var ms = new MemoryStream(source))
            {
                ms.Seek(0, SeekOrigin.Begin);
                var r = new BinaryReader(ms);

                for (int i = 0; i < EntryCount; i++)
                {
                    var bytes = r.ReadBytes(16);
                    if (bytes == null) throw new Exception("Failed to read doc guid");
                    _docIds[i] = new Guid(bytes);

                    _linkA[i] = new PageLink{
                        Version = new MonotonicByte(r.ReadByte()),
                        LastPage = r.ReadInt32()
                    };

                    _linkB[i] = new PageLink{
                        Version = new MonotonicByte(r.ReadByte()),
                        LastPage = r.ReadInt32()
                    };
                }
            }
        }
        
        /// <inheritdoc />
        public byte[] ToBytes()
        {
            using (var ms = new MemoryStream(PackedSize))
            {
                var w = new BinaryWriter(ms);

                for (int i = 0; i < EntryCount; i++)
                {
                    w.Write(_docIds[i].ToByteArray());

                    WriteLink(w, _linkA[i]);
                    WriteLink(w, _linkB[i]);
                }

                ms.Seek(0, SeekOrigin.Begin);
                return ms.ToArray();
            }
        }

        private void WriteLink([NotNull]BinaryWriter w, PageLink link)
        {
            if (link != null)
            {
                w.Write(link.Version?.Value ?? 0);
                w.Write(link.LastPage);
            }
            else
            {
                w.Write(0);
                w.Write(-1);
            }
        }

    }
}