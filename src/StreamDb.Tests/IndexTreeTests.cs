using System;
using System.Diagnostics;
using NUnit.Framework;
using StreamDb.Internal;
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
            var found = subject.Search(IndexPage.NeutralDocId, out _, out _);

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
            // no space to insert means add a new index page

            // Plan: add a whole load of entries, then measure an insert
            
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
        public void capacity_indication (){
            // We never expect to fill a page 100%.
            // This test run gives an indication of how the index behaves when filling
            
            var subject = new IndexPage();
            int i, j=0, k=0;
            var sw = new Stopwatch();
            sw.Start();
            for (i = 0; i < 1000; i++)
            {
                var ok = subject.TryInsert(Guid.NewGuid(), 123);
                if (ok) j++;
                if (!ok) k++;
            }
            sw.Stop();

            var capacity = j / 1.26;

            Console.WriteLine($"{i} attempts at random inserts. {j} succeeded ({capacity:0.#} % full), {k} failed; Took {sw.Elapsed}");
        }

        [Test]
        public void when_searching_and_an_entry_cant_be_found_we_get_a_failure_result()
        {
            var subject = new IndexPage();
            int i;
            for (i = 0; i < 200; i++) { subject.TryInsert(Guid.NewGuid(), 123); }

            var found = subject.Search(Guid.NewGuid(), out _, out _);
            Assert.That(found, Is.False, "Found a key that was not inserted");
        }

        [Test]
        public void when_searching_and_an_entry_is_found_we_get_the_entry_details (){
            // doc guid | start page LINK | end page LINK 
            // LINKs are two slots, each with a page-id and a version number

            
            var subject = new IndexPage();
            var key = Guid.NewGuid();

            // some 'wrong' keys (worst case occupancy is 6 out of 126)
            for (int i = 0; i < 5; i++) { subject.TryInsert(Guid.NewGuid(), 123); }

            // insert the target
            var ok = subject.TryInsert(key, 555); 
            Assert.That(ok, Is.True, "Failed to write target key");

            // fill up with other wrong keys
            for (int i = 0; i < 200; i++) { subject.TryInsert(Guid.NewGuid(), 123); }


            var found = subject.Search(key, out var linkA, out var linkB);
            Assert.That(found, Is.True, "Failed to find target");
            
            Assert.That(linkA.LastPage, Is.EqualTo(555), "Bad page info");
            Assert.That(linkA.Version.Value, Is.EqualTo(0), "Bad version info");

            Assert.That(linkB.LastPage, Is.EqualTo(-1), "New index has an old link!");
        }

        [Test]
        public void when_updating_an_entry_then_the_youngest_existing_value_is_kept_and_the_oldest_lost ()
        {
            Assert.Fail("NYI");
        }
    }
}