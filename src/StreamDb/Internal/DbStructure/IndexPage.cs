using System;
using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.DbStructure
{
    /// <summary>
    /// Content of a single index page
    /// </summary>
    public class IndexPage : IStreamSerialisable
    {

        const int EntryCount = 126; // 2+4+8+16+32+64
        const int PackedSize = 3276; // (16+5+5) * 126
        
        /// <summary> This is the implicit root index. It is not allowed as a real document ID </summary>
        public static readonly Guid NeutralDocId = new Guid(new byte[] { 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127 });
        /// <summary> This is an ID that means 'no document'. It is not allowed as a real document ID. </summary>
        public static readonly Guid ZeroDocId = Guid.Empty;


        [NotNull, ItemNotNull] private readonly VersionedLink[] _links;
        [NotNull] private readonly Guid[] _docIds;

        /*

            Layout: [ Doc Guid (16 bytes) | PageLink[0] (5 bytes) | PageLink[1] (5 bytes) ] --> 26 bytes
            We can fit 157 in a 4k page. Gives us 6 ranks (126 entries) -> 3276 bytes
            Our pages are currently 4061 bytes, so we have plenty of spare space if we can find useful metadata to store.

            We assume but don't store a root page with guid {127,127...,127}. The first two entries are 'left' and 'right' on the second level.

        */

        public IndexPage()
        {
            _links = new VersionedLink[EntryCount];
            for (int i = 0; i < EntryCount; i++) { _links[i] = new VersionedLink(); }

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
            _links[index].WriteNewLink(pageId, out _);
            _docIds[index] = docId;
            return true;

        }

        /// <summary>
        /// Try to find a link in this index page. Returns true if found, false if not found.
        /// If found, this will return up to two page options. Use the newest one with a valid CRC in the page.
        /// </summary>
        /// <param name="docId">Document ID to find</param>
        /// <param name="link">If found, this is the page link options. May be null</param>
        public bool Search(Guid docId, out VersionedLink link) {
            link = null;

            var index = Find(docId);
            if (index < 0 || index >= EntryCount) return false; // not found
            if (_docIds[index] == ZeroDocId) return false; // not found
            if (_docIds[index] != docId) throw new Exception("IndexPage.Search: Logic error");

            link = _links[index];

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

            _links[index].WriteNewLink(pageId, out expiredPage);
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
        public void Defrost(Stream source)
        {
            if (source == null || source.Length < PackedSize) throw new Exception("IndexPage.FromBytes: data was too short.");
            var r = new BinaryReader(source);

            for (int i = 0; i < EntryCount; i++)
            {
                var bytes = r.ReadBytes(16);
                if (bytes == null) throw new Exception("Failed to read doc guid");
                _docIds[i] = new Guid(bytes);


                _links[i].Defrost(r.BaseStream);
            }
        }

        /// <inheritdoc />
        public Stream Freeze()
        {
            var ms = new MemoryStream(PackedSize);
            var w = new BinaryWriter(ms);

            for (int i = 0; i < EntryCount; i++)
            {
                w.Write(_docIds[i].ToByteArray());
                _links[i].Freeze().CopyTo(ms);
            }

            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }
    }
}