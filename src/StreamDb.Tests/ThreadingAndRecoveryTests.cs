using System;
using System.IO;
using NUnit.Framework;
using StreamDb.Internal.DbStructure;
using StreamDb.Internal.Support;
using StreamDb.Tests.Helpers;
// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class ThreadingAndRecoveryTests {
        [Test, Repeat(8)]
        public void crashing_half_way_through_a_write_results_in_no_document_added_but_a_readable_database () {
            var rnd = new Random();

            // Cause a power-off like failure when writing a document.
            // We simulate this with a custom stream
            var baseStream = new MemoryStream();
            var stream = new CutoffStream(baseStream);

            var subject = Database.TryConnect(stream);

            // Write a good document, then a failure
            subject.WriteDocument("successful", MakeTestDocument());
            stream.CutoffAfter(rnd.Next(10, 900));
            subject.WriteDocument("failure", MakeTestDocument());

            Assert.That(stream.HasCutoff(), Is.True, "Failed to break output stream");

            // Load a new database from the truncated data
            baseStream.Rewind();
            var rawResult = baseStream.ToArray();
            var newStream = new MemoryStream(rawResult);

            var result = Database.TryConnect(newStream);
            var ok = result.Get("successful", out _);
            Assert.That(ok, Is.True, "Fully written document was lost");

            ok = result.Get("failure", out _);
            Assert.That(ok, Is.False, "Partly written document was loaded without an error");
        }

        [Test]
        public void if_a_document_is_lost_we_can_reuse_the_written_pages () {
            // Idea: when we allocate a page, or recover it from the free page list,
            // then we add it to the free page list with a 'journal' flag.
            // we can then wipe those when the write is good.


            // Cause a power-off like failure when writing a document.
            // We simulate this with a custom stream
            var baseStream = new MemoryStream();
            var stream = new CutoffStream(baseStream);

            var subject = Database.TryConnect(stream);

            // Write a good document, then a failure
            subject.WriteDocument("successful", MakeTestDocument());
            stream.CutoffAfter((int) (Page.PageDataCapacity * 1.9)); // make sure we have more than one page written
            subject.WriteDocument("failure", MakeTestDocument());

            Assert.That(stream.HasCutoff(), Is.True, "Failed to break output stream");

            // Load a new database from the truncated data
            baseStream.Rewind();
            var rawResult = baseStream.ToArray();
            var newStream = new MemoryStream(rawResult);

            var result = Database.TryConnect(newStream);

            result.CalculateStatistics(out var totalPages, out var freePages);
            Console.WriteLine($"Stats before writing: total pages = {totalPages}; free = {freePages}");

            Assert.That(freePages, Is.GreaterThan(0), "No free pages were found");
            
            // use some space and try again
            result.WriteDocument("successful2", MakeTestDocument());
            
            Console.WriteLine($"Stats after writing: total pages = {totalPages}; free = {freePages}");
            Assert.That(freePages, Is.Zero, "Free pages were not consumed");
        }

        [Test]
        public void when_a_document_crashes_during_an_update_the_previous_version_is_still_available () {
            Assert.Fail("NYI");
        }

        [Test]
        public void writing_documents_in_multiple_threads_works_correctly () {
            // Note: the path index is probably trickiest
            Assert.Fail("NYI");
        }

        [Test]
        public void reading_documents_in_multiple_threads_works_correctly () {
            Assert.Fail("NYI");
        }
        
        /// <summary>
        /// Makes a stream with 10kb of random data
        /// </summary>
        private static Stream MakeTestDocument()
        {
            var ms = new MemoryStream();
            var rnd= new Random();
            var buf = new byte[1024];
            for (int i = 0; i < 10; i++)
            {
                rnd.NextBytes(buf);
                ms.Write(buf, 0, buf.Length);
            }
            return ms.Rewind();
        }
    }
}