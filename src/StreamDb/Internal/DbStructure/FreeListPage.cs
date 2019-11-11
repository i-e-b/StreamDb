using System.IO;
using JetBrains.Annotations;
using StreamDb.Internal.Core;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.DbStructure
{
    /// <summary>
    /// Page structure for the free pages list
    /// </summary>
    /// <remarks>
    /// The free chain is a set of pages, each of which is just a big array of Int32 entries
    /// page zero is always occupied, and negative pages are invalid, so either of these is an empty slot in the free list
    ///
    /// Each free page can hold 1015 page IDs (3.9MB of document data space) -- so having multiples *should* be rare
    /// When searching for a free page, we scan the free chain first. If we can't find anything we
    /// allocate more space (writing off the end of the stream).
    ///
    /// Reading and writing free pages is done as close to the start of the chain as possible.
    /// The first free chain page is never removed, but the other pages can be removed when empty.
    ///
    /// Our database keeps up to 2 versions of each document, freeing pages as the third version 'expires',
    /// so in applications where updates happen a lot, we expect the free chain to be busy
    ///
    /// There is no clever data structures or algorithms here yet, it's just a scan.
    /// The free list provides no protection from double-free. The caller should check the returned page
    /// is not in use (with page type and document id).
    /// </remarks>
    public class FreeListPage: IStreamSerialisable
    {
        [NotNull]private readonly int[] _entries;
        public const int Capacity = Page.PageDataCapacity / sizeof(int);

        public FreeListPage()
        {
            _entries = new int[Capacity];
        }

        /// <summary>
        /// Return a free page if it can be found. Returns -1 if no free pages are available.
        /// The free page will be removed from the list as part of the get call.
        /// </summary>
        public bool TryGetNext(out int id)
        {
            id = -1;
            for (int i = 0; i < Capacity; i++)
            {
                if (_entries[i] < 3) continue;

                id = _entries[i];
                _entries[i] = 0;
                return true;
            }
            return false;
        }
        
        /// <summary>
        /// Try to add a new free page to the list. Returns true if it worked, false if there was no free space
        /// </summary>
        public bool TryAdd(int pageId)
        {
            if (pageId < 3) return false;
            for (int i = 0; i < Capacity; i++)
            {
                if (_entries[i] == pageId) {
                    return true;
                }
                if (_entries[i] > 3) continue;

                _entries[i] = pageId;
                return true;
            }
            return false;
        }

        /// <inheritdoc />
        public Stream Freeze()
        {
            var ms = new MemoryStream(Capacity * sizeof(int));
            var w = new BinaryWriter(ms);
            for (int i = 0; i < _entries.Length; i++)
            {
                w.Write(_entries[i]);
            }
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

        /// <inheritdoc />
        public void Defrost(Stream source)
        {
            if (source == null) return;
            var r = new BinaryReader(source);
            for (int i = 0; i < _entries.Length; i++)
            {
                _entries[i] = r.ReadInt32();
            }
        }

        /// <summary>
        /// Scan the page, returning a count of free pages.
        /// This is for statistics. Use `GetNext` to read a free page index
        /// </summary>
        public int Count()
        {
            var count = 0;
            for (int i = 0; i < Capacity; i++)
            {
                if (_entries[i] >= 3) count++;
            }
            return count;
        }
    }
}