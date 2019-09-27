using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using NUnit.Framework;
using StreamDb.Internal.DbStructure;

// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class PageTableTests {

        [Test]
        public void create_page_table_in_stream () {
            var ms = new MemoryStream();
            var subject = new PageTable(ms);

            Assert.That(ms.Length, Is.EqualTo(Page.PageRawSize * 4), "Unexpected empty DB size");
            Console.WriteLine($"Empty database consumes {(ms.Length / 1024)}kB");

            var page0 = subject.GetPageRaw(0);
            Assert.That(page0, Is.Not.Null, "Failed to read root page");
            Assert.That(page0.ValidateCrc(), Is.True, "Root page CRC is wrong");

            var page1 = subject.GetPageRaw(1);
            Assert.That(page1, Is.Not.Null, "Failed to read first index page");
            Assert.That(page1.ValidateCrc(), Is.True, "First index page CRC is wrong");
        }

        [Test]
        public void can_allocate_pages() {
            var ms = new MemoryStream();
            var subject = new PageTable(ms);

            var page_n = subject.GetFreePage();
            Assert.That(page_n, Is.Not.Null, "Page not provided");
            Assert.That(page_n.FirstPageId, Is.GreaterThan(3), "Unexpected page index");
        }

        [Test]
        public void can_read_a_page_table_from_a_stream () {
            byte[] raw;
            int pageId;
            var sample = Encoding.UTF8.GetBytes("Hello, world");

            using (var source = new MemoryStream()){
                var creator = new PageTable(source);
                var testPage = creator.GetFreePage();

                pageId = testPage.OriginalPageId;
                testPage.FirstPageId = pageId;
                testPage.PageType = PageType.Data;
                testPage.DocumentId = Guid.NewGuid();
                testPage.Write(sample, 0, 0, sample.Length);
                testPage.UpdateCRC();
                creator.CommitPage(testPage);

                source.Seek(0, SeekOrigin.Begin);
                raw = source.ToArray();
            }

            using (var stored = new MemoryStream(raw)) {
                stored.Seek(0, SeekOrigin.Begin);
                var reader = new PageTable(stored);

                var resultPage = reader.GetPageRaw(pageId);

                Assert.That(resultPage, Is.Not.Null, "Failed to read result page");
                
                var result = new byte[sample.Length];
                resultPage.Read(result, 0, 0, result.Length);

                var finalStr = Encoding.UTF8.GetString(result);
                Assert.That(finalStr, Is.EqualTo("Hello, world"));
            }
        }


        [Test]
        public void can_write_and_read_data_pages_from_a_stream () {
            using (var fileDataStream = new MemoryStream())
            using (var actualStream = new MemoryStream())
            using (var ms = new MemoryStream()){
                var subject = new PageTable(ms);

                // prepare a data stream that will span multiple pages
                for (int i = 0; i < Page.PageDataCapacity * 3.5; i++) // ensure we write a partial page at the end
                {
                    fileDataStream.WriteByte(unchecked((byte)i));
                }
                fileDataStream.Seek(0, SeekOrigin.Begin);
                var sw = new Stopwatch();

                // write it to the DB
                sw.Start();
                var docID = subject.WriteDocument(fileDataStream);
                sw.Stop();
                Console.WriteLine($"Write took {sw.Elapsed}");

                // read it back
                sw.Restart();
                var resultStream = subject.ReadDocument(docID);
                sw.Stop();
                Console.WriteLine($"Seek took {sw.Elapsed}");

                Assert.That(resultStream, Is.Not.Null, "Failed to find the document");

                // compare the results
                fileDataStream.Seek(0, SeekOrigin.Begin);
                var expected = fileDataStream.ToArray();
                
                sw.Restart();
                resultStream.CopyTo(actualStream);
                sw.Stop();
                Console.WriteLine($"File transfer took {sw.Elapsed}");

                actualStream.Seek(0, SeekOrigin.Begin);
                var actual = actualStream.ToArray();

                Assert.That(actual, Is.EquivalentTo(expected), "Data was damaged during storage");
            }
        }

        [Test]
        public void can_delete_a_chain_of_pages_from_its_start () {
            using (var fileDataStream = new MemoryStream())
            using (var ms = new MemoryStream()){
                var subject = new PageTable(ms);

                // prepare a data stream that will span multiple pages
                for (int i = 0; i < Page.PageDataCapacity * 3; i++)
                {
                    fileDataStream.WriteByte(unchecked((byte)i));
                }
                fileDataStream.Seek(0, SeekOrigin.Begin);

                // write it to the DB
                var docID = subject.WriteDocument(fileDataStream);

                var pageId = subject.GetPageIdFromDocumentId(docID);
                Assert.That(pageId, Is.GreaterThan(0), "Failed to find page");

                // find the first page
                var firstId = subject.GetPageRaw(pageId).FirstPageId;

                // delete the chain
                subject.DeletePageChain(firstId);

                // next free page should be the first ID
                var result = subject.GetFreePage();
                Assert.That(result.OriginalPageId, Is.EqualTo(firstId));
            }
        }

        [Test]
        public void can_delete_a_chain_of_pages_from_its_middle () {
            using (var fileDataStream = new MemoryStream())
            using (var ms = new MemoryStream()){
                var subject = new PageTable(ms);

                // prepare a data stream that will span multiple pages
                for (int i = 0; i < Page.PageDataCapacity * 3; i++)
                {
                    fileDataStream.WriteByte(unchecked((byte)i));
                }
                fileDataStream.Seek(0, SeekOrigin.Begin);

                // write it to the DB
                var docID = subject.WriteDocument(fileDataStream);

                var endId = subject.GetPageIdFromDocumentId(docID);
                Assert.That(endId, Is.GreaterThan(0), "Failed to find page");

                // find the first page
                var firstId = subject.GetPageRaw(endId).FirstPageId;
                var nextId = subject.GetPageRaw(firstId).NextPageId;

                // delete the chain
                subject.DeletePageChain(nextId);

                // next free page should be the first ID
                var result = subject.GetFreePage();
                Assert.That(result.OriginalPageId, Is.EqualTo(firstId));
            }
        }

        [Test]
        public void can_delete_a_chain_of_pages_from_its_end () {
            using (var fileDataStream = new MemoryStream())
            using (var ms = new MemoryStream()){
                var subject = new PageTable(ms);

                // prepare a data stream that will span multiple pages
                for (int i = 0; i < Page.PageDataCapacity * 3; i++)
                {
                    fileDataStream.WriteByte(unchecked((byte)i));
                }
                fileDataStream.Seek(0, SeekOrigin.Begin);

                // write it to the DB
                var docID = subject.WriteDocument(fileDataStream);

                var endId = subject.GetPageIdFromDocumentId(docID);
                Assert.That(endId, Is.GreaterThan(0), "Failed to find page");

                // find the first page
                var firstId = subject.GetPageRaw(endId).FirstPageId;

                // delete the chain
                subject.DeletePageChain(endId);

                // next free page should be the first ID
                var result = subject.GetFreePage();
                Assert.That(result.OriginalPageId, Is.EqualTo(firstId));
            }
        }


        [Test]
        public void can_write_a_path_entry_for_a_document_id ()
        {
            var sw = new Stopwatch();
            using (var ms = new MemoryStream()){
                var subject = new PageTable(ms);

                var doc1 = Guid.NewGuid();
                var doc2 = Guid.NewGuid();

                sw.Restart();
                var old = subject.BindPathToDocument("my/first/path", doc1);
                sw.Stop();
                Assert.That(old, Is.EqualTo(Guid.Empty), $"Got an unexpected result: {old}");
                Console.WriteLine($"First write took {sw.Elapsed}");

                sw.Restart();
                var result = subject.BindPathToDocument("my/first/path", doc2);
                sw.Stop();
                Assert.That(result, Is.EqualTo(doc1), $"Expected {doc1}, but got {result}");
                Console.WriteLine($"Second write took {sw.Elapsed}");
            }
        }

        [Test]
        public void can_query_a_document_by_path ()
        {
            byte[] frozenDB;
            Guid originalId;

            using (var fileDataStream = new MemoryStream())
            using (var ms = new MemoryStream()){
                var subject = new PageTable(ms);

                // prepare a data stream that will span multiple pages
                for (int i = 0; i < Page.PageDataCapacity * 3; i++)
                {
                    fileDataStream.WriteByte(unchecked((byte)i));
                }
                fileDataStream.Seek(0, SeekOrigin.Begin);

                // write it to the DB
                originalId = subject.WriteDocument(fileDataStream);

                // #### Write a path ####
                subject.BindPathToDocument("the document I want", originalId);


                // write everything to a byte array
                ms.Seek(0, SeekOrigin.Begin);
                frozenDB = ms.ToArray();
            }

            // Read the DB back
            using (var ms2 = new MemoryStream(frozenDB)){
                var subject = new PageTable(ms2);

                // find doc by path
                var docId = subject.GetDocumentIdByPath("the document I want");
                Assert.That(docId, Is.EqualTo(originalId), "Failed to find document by path");
                
                // read the file back it back
                var resultStream = subject.ReadDocument(docId);

                Assert.That(resultStream, Is.Not.Null, "Failed to find the document");
            }
        }

    }
}