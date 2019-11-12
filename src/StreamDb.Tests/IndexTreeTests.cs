using System;
using System.Diagnostics;
using NUnit.Framework;
using StreamDb.Internal.Core;
using StreamDb.Internal.DbStructure;

// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class IndexTreeTests 
    {
        [Test]
        public void empty_tree_does_not_find_implicit_node (){
            // The *implicit* root node is ID 127...127
            // the actual tree then starts with the left and right from that.

            // No real document is allowed to have Guid.Zero as an ID.
            // If Guid.Zero exists in the tree, that's the end of the branch

            var subject = new IndexPage();
            var found = subject.Search(IndexPage.NeutralDocId, out _);

            Assert.That(found, Is.False, "Search revealed the phantom root node");
        }

        [Test]
        public void when_inserting_and_a_space_is_found_then_that_space_is_written_to (){
            // and we get a success result
            var subject = new IndexPage();

            var ok = subject.TryInsert(Guid.NewGuid(), 123);

            Assert.That(ok, Is.True, "Insertion failed");
        }

        [Test]
        public void when_inserting_and_a_space_cant_be_found_we_get_a_failure_result () {
            // when an insert fails, we expect the caller to try a different index page.
            // it does NOT mean the index is full. The caller should try adding again with
            // the next insert. We expect that the entire index-tree-chain is traversed
            // before adding a new index page (if all return false)
            
            var subject = new IndexPage();
            int i;
            for (i = 0; i < 200; i++)
            {
                var ok = subject.TryInsert(Guid.NewGuid(), 123);
                if (!ok) break;
            }

            Assert.That(i, Is.LessThan(127), "Index page exceeded real capacity");
            Console.WriteLine($"Managed {i} random inserts, out of a potential 126");
        }

        [Test]
        public void  a_filled_index_serialises_to_less_than_a_pages_data_limit () {
            // We rarely expect to fill a page 100%.
            // This test run gives an indication of how the index behaves when filling
            // worst case is IDs in order, where we will get 6 entries (4.7% full)
            
            var subject = new IndexPage();
            int i, j=0, k=0;
            var sw = new Stopwatch();
            sw.Start();
            for (i = 0; i < 1000; i++) // with 1'000'000 inserts, we hit 100% most of the time. With 1'000 we are around 80% most of the time
            {
                var ok = subject.TryInsert(Guid.NewGuid(), 123);
                if (ok) j++;
                if (!ok) k++;
            }
            sw.Stop();

            var capacity = j / 1.26;

            Console.WriteLine($"{i} attempts at random inserts. {j} succeeded ({capacity:0.#} % full), {k} failed; Took {sw.Elapsed}");

            var bytes = subject.Freeze();
            Assert.That(bytes.Length, Is.LessThan(ComplexPage.PageDataCapacity));
            Console.WriteLine($"Index page is {bytes.Length} bytes (out of {ComplexPage.PageDataCapacity})");
        }

        [Test]
        public void when_searching_and_an_entry_cant_be_found_we_get_a_failure_result()
        {
            var subject = new IndexPage();
            int i;
            for (i = 0; i < 200; i++) { subject.TryInsert(Guid.NewGuid(), 123); }

            var found = subject.Search(Guid.NewGuid(), out _);
            Assert.That(found, Is.False, "Found a key that was not inserted");
        }

        [Test]
        public void when_searching_and_an_entry_is_found_we_get_the_entry_details (){
            var subject = new IndexPage();
            var key = Guid.NewGuid();

            // some 'wrong' keys (worst case occupancy is 6 out of 126)
            for (int i = 0; i < 5; i++) { subject.TryInsert(Guid.NewGuid(), 123); }

            // insert the target
            var ok = subject.TryInsert(key, 555); 
            Assert.That(ok, Is.True, "Failed to write target key");

            // fill up with other wrong keys
            for (int i = 0; i < 200; i++) { subject.TryInsert(Guid.NewGuid(), 123); }


            var found = subject.Search(key, out var link);
            Assert.That(found, Is.True, "Failed to find target");
            
            link.TryGetLink(0, out var newPageId);
            Assert.That(newPageId, Is.EqualTo(555), "Bad page info");

            link.TryGetLink(1, out var oldPageId);
            Assert.That(oldPageId, Is.EqualTo(-1), "New index has an old link!");
        }
        

        [Test]
        public void index_pages_can_be_read_after_serialisation (){
            var original = new IndexPage();
            var key = Guid.NewGuid();

            // some 'wrong' keys (worst case occupancy is 6 out of 126)
            for (int i = 0; i < 5; i++) { original.TryInsert(Guid.NewGuid(), 123); }

            // insert the target
            var ok = original.TryInsert(key, 555); 
            Assert.That(ok, Is.True, "Failed to write target key");

            // fill up with other wrong keys
            for (int i = 0; i < 200; i++) { original.TryInsert(Guid.NewGuid(), 123); }

            var bytes = original.Freeze();

            var result = new IndexPage();
            result.Defrost(bytes);

            var found = result.Search(key, out var link);
            Assert.That(found, Is.True, "Failed to find target");
            
            link.TryGetLink(0, out var newPageId);
            Assert.That(newPageId, Is.EqualTo(555), "Bad page info");

            link.TryGetLink(1, out var oldPageId);
            Assert.That(oldPageId, Is.EqualTo(-1), "New index has an old link!");
        }

        [Test]
        public void inserting_the_same_document_id_twice_throws_an_exception () {
            var subject = new IndexPage();

            var key = Guid.NewGuid();

            subject.TryInsert(key, 1);
            Assert.Throws<Exception>(() => { subject.TryInsert(key, 3); }, "Second insert should have thrown, but didn't");
        }

        [Test]
        public void attempting_to_update_a_missing_entry_returns_an_invalid_result()
        {
            var subject = new IndexPage();
            for (int i = 0; i < 200; i++) { subject.TryInsert(Guid.NewGuid(), 123); }

            var key = Guid.NewGuid();
            var ok = subject.Update(key, 3, out _);

            // we return false, expecting caller to continue through the index chain.
            // caller should error if they get to the end of the chain without success.

            Assert.That(ok, Is.False, "Index should have rejected update, but did not");
        }

        [Test]
        public void when_updating_an_entry_then_the_youngest_existing_value_is_kept_and_the_oldest_lost ()
        {
            // Setup
            var subject = new IndexPage();
            for (int i = 0; i < 5; i++) { subject.TryInsert(Guid.NewGuid(), 123); }
            var key = Guid.NewGuid();
            subject.TryInsert(key, 1);


            // Update 1
            var ok = subject.Update(key, 2, out var exp1);
            Assert.That(ok, Is.True, "Update failed");
            var found = subject.Search(key, out var link);
            Assert.That(found, Is.True, "Updated value was lost");
            Assert.That(exp1, Is.EqualTo(-1), "Expected no pages expired, but found one");

            link.TryGetLink(0, out var pageId);
            Assert.That(pageId, Is.EqualTo(2), "new value wrong");

            link.TryGetLink(1, out pageId);
            Assert.That(pageId, Is.EqualTo(1), "old value wrong");

            // Update 2
            ok = subject.Update(key, 3, out var exp2);
            Assert.That(ok, Is.True, "Update failed");
            found = subject.Search(key, out link);
            Assert.That(found, Is.True, "Updated value was lost");
            Assert.That(exp2, Is.EqualTo(1), $"Expected first page expired, but was {exp2}");

            link.TryGetLink(0, out pageId);
            Assert.That(pageId, Is.EqualTo(3), "new value wrong");

            link.TryGetLink(1, out pageId);
            Assert.That(pageId, Is.EqualTo(2), "old value wrong");
            
            // Update 3
            ok = subject.Update(key, 4, out var exp3);
            Assert.That(ok, Is.True, "Update failed");
            found = subject.Search(key, out link);
            Assert.That(found, Is.True, "Updated value was lost");
            Assert.That(exp3, Is.EqualTo(2), $"Expected first page expired, but was {exp3}");

            link.TryGetLink(0, out pageId);
            Assert.That(pageId, Is.EqualTo(4), "new value wrong");

            link.TryGetLink(1, out pageId);
            Assert.That(pageId, Is.EqualTo(3), "old value wrong");

            // ... and it flip-flops from there
        }
    }
}