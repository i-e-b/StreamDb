using System;
using NUnit.Framework;
using StreamDb.Internal;

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
        public void when_searching_and_an_entry_cant_be_found_we_get_a_failure_result () {
            Assert.Fail("NYI");

        }

        [Test]
        public void when_searching_and_an_entry_is_found_we_get_the_entry_details (){
            // doc guid | start page LINK | end page LINK 
            // LINKs are two slots, each with a page-id and a version number
            Assert.Fail("NYI");
        }
    }
}