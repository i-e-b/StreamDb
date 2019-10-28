using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using DispatchSharp;
using NUnit.Framework;
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

            var subject = Database_OLD.TryConnect(stream);

            // Write a good document, then a failure
            subject.WriteDocument("successful", MakeTestDocument());
            stream.CutoffAfter(rnd.Next(10, 900));
            try {
                subject.WriteDocument("failure", MakeTestDocument());
            }
            catch {
                // ignore
            }

            Assert.That(stream.HasCutoff(), Is.True, "Failed to break output stream");

            // Load a new database from the truncated data
            baseStream.Rewind();
            var rawResult = baseStream.ToArray();
            var newStream = new MemoryStream(rawResult);

            var result = Database_OLD.TryConnect(newStream);
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
            Assert.Inconclusive("Will revisit when I have a better journal design.");
/*
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
            */
        }

        [Test]
        public void when_a_document_crashes_during_an_update_the_previous_version_is_still_available () {
            var rnd = new Random();

            // Cause a power-off like failure when writing a document.
            // We simulate this with a custom stream
            var baseStream = new MemoryStream();
            var stream = new CutoffStream(baseStream);

            var subject = Database_OLD.TryConnect(stream);

            // Write a good document, then a failure
            subject.WriteDocument("repeat", MakeTestDocument());
            stream.CutoffAfter(rnd.Next(10, 900));
            try {
                subject.WriteDocument("repeat", MakeTestDocument());
            } catch {
                // Ignore
            }

            Assert.That(stream.HasCutoff(), Is.True, "Failed to break output stream");

            // Load a new database from the truncated data
            baseStream.Rewind();
            var rawResult = baseStream.ToArray();
            var newStream = new MemoryStream(rawResult);

            var result = Database_OLD.TryConnect(newStream);
            var ok = result.Get("repeat", out var resultData);
            Assert.That(ok, Is.True, "Fully written document was lost");

            var temp = new MemoryStream();
            resultData.CopyTo(temp); // this will cause each page's CRC to be tested.
        }

        [Test]
        public void writing_documents_in_multiple_threads_works_correctly () {
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

                var dispatcher = Dispatch<int>.CreateDefaultMultithreaded("MyTask", threadCount: 10);

                const int rounds = 50;
                for (int i = 0; i < rounds; i++) dispatcher.AddWork(i);

                dispatcher.AddConsumer(i=>{
                    var j = (5 * i) % rounds;
                    Console.Write($"W{i},R{j}; ");
                    // ReSharper disable AccessToDisposedClosure
                    subject.WriteDocument($"test/data-path/{i}", MakeTestDocument());
                    subject.Get($"test/data-path/{j}", out _);
                    // ReSharper restore AccessToDisposedClosure
                });

                dispatcher.Start();
                dispatcher.WaitForEmptyQueueAndStop();

                subject.Flush();
                ms.Rewind();
                var rawData = ms.ToArray();

                // Check we can still load and read the database
                var result = Database_OLD.TryConnect(new MemoryStream(rawData));

                Console.WriteLine(string.Join(", ", result.Search("test")));

                var failed = new List<int>();

                for (int i = 0; i < rounds; i++)
                {
                    var ok = result.Get($"test/data-path/{i}", out _);
                    if (!ok) failed.Add(i);
                }

                Assert.That(failed, Is.Empty, "Failed lookups: " + string.Join(", ", failed));
            }
        }

        [Test]
        public void reading_documents_in_multiple_threads_works_correctly () {
            using (var doc = MakeTestDocument())
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

                Console.WriteLine("Writing doc");
                doc.Seek(0, SeekOrigin.Begin);
                subject.WriteDocument("test/data-path/doc", doc);

                var dispatcher = Dispatch<int>.CreateDefaultMultithreaded("MyTask", threadCount: 10);

                for (int i = 0; i < 500; i++) dispatcher.AddWork(i);

                dispatcher.AddConsumer(i=>{
                    //Thread.Sleep(i % 8);
                    Console.Write($"{i}, ");
                    subject.Get("test/data-path/doc", out _);
                    subject.Get("this document is not here", out _); 
                });

                dispatcher.Start();
                dispatcher.WaitForEmptyQueueAndStop(TimeSpan.FromSeconds(10));
            }
        }



        public class IEDP {
            public delegate int GetNextPage();

            private volatile GetNextPage _pageDelegate;

            public IEDP()
            {
                i = 0;
                _pageDelegate = SelfPageDelegate;
            }

            public int TryGetNextPage() {
                GetNextPage act = null;

                while (act == null) {
                    act = Interlocked.Exchange(ref _pageDelegate, null);
                }
                try {
                    return act();
                } finally {
                    var old = Interlocked.Exchange(ref _pageDelegate, act);
                    if (old != null) throw new Exception("Interlock failed in IEDP");
                }
            }

            private int i;
            private int SelfPageDelegate()
            {
                return i++;
            }
        }


        [Test]
        public void interlock_exchange_delegate_pointer ()
        {
            var subject = new IEDP();
            var switches = new bool[500];

            var dispatcher = Dispatch<int>.CreateDefaultMultithreaded($"TEST: {nameof(interlock_exchange_delegate_pointer)}", threadCount: 10);
            for (int i = 0; i < 500; i++) dispatcher.AddWork(i);

            dispatcher.AddConsumer(i=>{
                Thread.Sleep(i%20);
                var x = subject.TryGetNextPage();
                Console.Write($"{i}=>{x}, ");
                switches[x] = true;
            });

            dispatcher.Start();
            dispatcher.WaitForEmptyQueueAndStop(TimeSpan.FromSeconds(10));

            Assert.That(switches.All(v=>v), Is.True, "Not all switches were set");


            // Thought... keep a pool of about 32 slots that we keep free page indexes in. (that's about 128kb worth)
            // Each requester goes through the IEDP to get a number from 0..31, and pulls the page from there.
            // If there isn't a page in our slot, we hit the slow path and rebuild the slots.
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