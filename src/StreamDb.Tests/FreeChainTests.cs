using NUnit.Framework;
using StreamDb.Internal.DbStructure;
using StreamDb.Internal.Support;
// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class FreeChainTests {
        [Test]
        public void default_free_chain_is_empty () {
            var subject = new FreeListPage();

            var found = subject.GetNext();
            Assert.That(found, Is.EqualTo(-1), "Got a valid page ID from an empty list");
        }

        [Test]
        public void adding_a_page_to_a_list_with_space_is_accepted () {
            var subject = new FreeListPage();

            bool ok = subject.TryAdd(123);

            Assert.That(ok, Is.True, "Add was rejected");
        }
        

        [Test]
        public void can_consume_an_added_page () {
            var subject = new FreeListPage();

            subject.TryAdd(123);
            subject.TryAdd(234);

            var found = subject.GetNext();

            Assert.That(found, Is.Not.EqualTo(-1), "Lost free page");
        }

        [Test]
        public void adding_a_page_to_a_full_list_is_rejected () {
            Assert.Fail("NYI");
        }

        [Test]
        public void adding_and_consuming_pages_out_of_sequence_works () {
            Assert.Fail("NYI");
        }

        [Test]
        public void free_table_survives_serialisation () {
            Assert.Fail("NYI");
        }
    }

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
    /// </remarks>
    public class FreeListPage: IByteSerialisable
    {
        private readonly int[] _entries;
        const int Capacity = Page.PageDataCapacity / sizeof(int);

        public FreeListPage()
        {
            _entries = new int[Capacity];
        }

        /// <summary>
        /// Return a free page if it can be found. Returns -1 if no free pages are available.
        /// The free page will be removed from the list as part of the get call.
        /// </summary>
        public int GetNext()
        {
            for (int i = 0; i < Capacity; i++)
            {
                if (_entries[i] <= 3) continue;

                var found = _entries[i];
                _entries[i] = 0;
                return found;
            }
            return -1;
        }
        
        /// <summary>
        /// Try to add a new free page to the list. Returns true if it worked, false if there was no free space
        /// </summary>
        public bool TryAdd(int pageId)
        {
            for (int i = 0; i < Capacity; i++)
            {
                if (_entries[i] > 3) continue;

                _entries[i] = pageId;
                return true;
            }
            return false;
        }

        /// <inheritdoc />
        public byte[] ToBytes()
        {
            return null;//TODO:_IMPLEMENT_ME;
        }

        /// <inheritdoc />
        public void FromBytes(byte[] source)
        {
            //TODO:_IMPLEMENT_ME();
        }
    }
}