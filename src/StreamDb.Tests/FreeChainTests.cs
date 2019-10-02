using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using StreamDb.Internal.DbStructure;

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
        public void adding_a_static_page_is_rejected () {
            var subject = new FreeListPage();

            bool ok = subject.TryAdd(1);

            Assert.That(ok, Is.False, "Add was accepted, but should have been rejected");
        }

        [Test]
        public void adding_a_negative_page_id_is_rejected () {
            var subject = new FreeListPage();

            bool ok = subject.TryAdd(-1);

            Assert.That(ok, Is.False, "Add was accepted, but should have been rejected");
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

            var subject = new FreeListPage();
            int i;
            for (i = 0; i < FreeListPage.Capacity * 2; i++)
            {
                if (!subject.TryAdd(i + 4)) break;
            }

            Assert.That(i, Is.EqualTo(FreeListPage.Capacity), "Free list did not stop at limit");
        }

        [Test]
        public void adding_and_consuming_pages_out_of_sequence_works () {
            var expected = new HashSet<int>();
            var rnd = new Random();
            var subject = new FreeListPage();

            int v;
            for (int i = 0; i < 1000; i++)
            {
                var q = rnd.Next(0, 2);
                switch (q) {
                    case 0:
                        v = rnd.Next(4,50000);
                        if (!expected.Contains(v) && subject.TryAdd(v)) {
                            expected.Add(v);
                        }
                        Console.Write("+");
                        break;

                    case 1:
                        v = subject.GetNext();
                        if (v < 0) break;
                        if (!expected.Contains(v)) {
                            Assert.Fail("Unexpected value");
                        }
                        expected.Remove(v);
                        Console.Write("-");
                        break;
                }
            }

        }

        [Test]
        public void free_table_survives_serialisation () {
            var added = new List<int>();
            var retrieved = new List<int>();
            var original = new FreeListPage();
            int i;
            for (i = 10; i < 100; i += i / 10)
            {
                Console.Write($"{i}, ");
                added.Add(i);
                original.TryAdd(i);
            }

            var bytes = original.ToBytes();
            var result = new FreeListPage();
            result.FromBytes(new MemoryStream(bytes));

            for (i = 0; i < 100; i++)
            {
                var free = result.GetNext();
                if (free > 0) retrieved.Add(free);
                else break;
            }

            Assert.That(retrieved, Is.EquivalentTo(added), "Free list was corrupted");
        }
    }
}