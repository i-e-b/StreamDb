﻿using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using StreamDb.Internal.Core;
// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class PageStreamStorageTests {
        [Test]
        public void create_from_memory_stream () {
            var storage = new MemoryStream();
            var sampleData = new byte[] { 1, 4, 7, 2, 5, 8, 3, 6, 9 };
            var sampleDataStream = new MemoryStream(sampleData);
            sampleDataStream.Seek(0, SeekOrigin.Begin);

            var subject = new PageStreamStorage(storage);
            
            Console.WriteLine($"Storage after headers is {storage.Length} bytes");

            var pageId = subject.WriteStream(sampleDataStream);

            Console.WriteLine($"Storage after writing data is {storage.Length} bytes");

            Assert.That(storage.Length, Is.GreaterThan(0), "Storage was not written");
            Assert.That(pageId, Is.GreaterThanOrEqualTo(0), "Bad page ID");

            var result = subject.GetStream(pageId);
            Assert.That(result, Is.Not.Null, "Failed to read stream");
            Assert.That(result.Length, Is.EqualTo(sampleData.Length), "Data length was wrong");

            var final = new byte[result.Length];
            var read = result.Read(final, 0, final.Length);
            Assert.That(read, Is.EqualTo(final.Length), "Data was not read to end");
            Assert.That(final, Is.EquivalentTo(sampleData), "Read and written data were different");
        }
         
        [Test]
        public void cycling_page_usage()
        {
            var storage = new MemoryStream();
            var sampleData = new byte[] { 1, 4, 7, 2, 5, 8, 3, 6, 9 };
            var sampleDataStream = new MemoryStream(sampleData);
            sampleDataStream.Seek(0, SeekOrigin.Begin);

            var subject = new PageStreamStorage(storage);
            
            Console.WriteLine($"Storage after headers is {storage.Length} bytes");

            for (int i = 0; i < 10; i++)
            {
                sampleDataStream.Seek(0, SeekOrigin.Begin);
                var pageId = subject.WriteStream(sampleDataStream);
                subject.ReleaseChain(pageId);
            }

            Console.WriteLine($"Storage after writing data is {storage.Length} bytes");
        }

        [Test]
        public void reusing_large_page_chains () {

            var storage = new MemoryStream();
            var sampleData = new byte[32767];
            for (int i = 0; i < sampleData.Length; i++) { sampleData[i] = (byte)i; }

            var sampleDataStream = new MemoryStream(sampleData);
            sampleDataStream.Seek(0, SeekOrigin.Begin);

            var subject = new PageStreamStorage(storage);
            
            Console.WriteLine($"Storage after headers is {storage.Length} bytes");

            var toRelease = new Queue<int>();

            for (int i = 0; i < 10; i++)
            {
                sampleDataStream.Seek(0, SeekOrigin.Begin);
                toRelease.Enqueue(subject.WriteStream(sampleDataStream));

                if (toRelease.Count > 2) {
                    subject.ReleaseChain(toRelease.Dequeue());
                }
            }

            Console.WriteLine($"Storage after writing data is {storage.Length} bytes");
        }

        [Test]
        public void freeing_a_large_number_of_pages()
        {
            var storage = new MemoryStream();
            var sampleData = new byte[1];
            for (int i = 0; i < sampleData.Length; i++) { sampleData[i] = (byte)i; }

            var sampleDataStream = new MemoryStream(sampleData);
            sampleDataStream.Seek(0, SeekOrigin.Begin);

            var subject = new PageStreamStorage(storage);
            
            Console.WriteLine($"Storage after headers is {storage.Length} bytes");

            var toRelease = new Queue<int>();

            for (int i = 0; i < 3000; i++) // a free page can hold about 1020 page refs
            {
                sampleDataStream.Seek(0, SeekOrigin.Begin);
                toRelease.Enqueue(subject.WriteStream(sampleDataStream));
            }

            while (toRelease.Count > 0) {
                subject.ReleaseChain(toRelease.Dequeue());
            }

            Console.WriteLine($"Storage after writing data is {storage.Length} bytes");

            // Try to reuse the pages
            for (int i = 0; i < 3000; i++)
            {
                sampleDataStream.Seek(0, SeekOrigin.Begin);
                toRelease.Enqueue(subject.WriteStream(sampleDataStream));
            }

            Console.WriteLine($"Storage after re-writing data is {storage.Length} bytes");
        }

        [Test]
        public void writing_to_index ()
        {
            var storage = new MemoryStream();
            var subject = new PageStreamStorage(storage);

            var newPageId = 123;
            var docId = Guid.NewGuid();
            subject.SetDocument(docId, newPageId, out var oldPageId);

            var result = subject.GetDocumentHead(docId);

            Assert.That(result, Is.EqualTo(newPageId));
            Assert.That(oldPageId, Is.EqualTo(-1));
        }

        [Test]
        public void writing_many_pages_to_the_index () {
            var storage = new MemoryStream();
            var subject = new PageStreamStorage(storage);

            
            var firstPageId = 123;
            var firstDocId = Guid.NewGuid();
            subject.SetDocument(firstDocId, firstPageId, out _);

            for (int i = 0; i < 1000; i++)
            {
                subject.SetDocument(Guid.NewGuid(), i, out _);
            }
            
            var lastPageId = 123;
            var lastDocId = Guid.NewGuid();
            subject.SetDocument(lastDocId, lastPageId, out _);


            Assert.That(subject.GetDocumentHead(firstDocId), Is.EqualTo(firstPageId));
            Assert.That(subject.GetDocumentHead(lastDocId), Is.EqualTo(lastPageId));
            
            Console.WriteLine($"Storage after writing data is {storage.Length} bytes");
        }

        [Test]
        public void path_lookup_data () {
            var storage = new MemoryStream();
            var subject = new PageStreamStorage(storage);

            var val1 = Guid.NewGuid();
            var val2 = Guid.NewGuid();

            subject.BindPath("this is my path", val1, out var old1);
            subject.BindPath("this is another path", val2, out var old2);

            var result1 = subject.GetDocumentIdByPath("this is my path");
            var result2 = subject.GetDocumentIdByPath("this path is not presend");

            Assert.That(result1, Is.EqualTo(val1));
            Assert.That(result2, Is.Null);
        }

        [Test]
        public void path_replacement_cycling()
        {
            var storage = new MemoryStream();
            var subject = new PageStreamStorage(storage);

            var valIn = Guid.NewGuid();
            Guid? prev = null;

            for (int i = 0; i < 10; i++)
            {
                subject.BindPath("this path will get replaced a lot", valIn, out var valOut);
                Assert.That(valOut, Is.EqualTo(prev), $"Failed on cycle {i}");
                prev = valIn;
                valIn = Guid.NewGuid();
            }
            Console.WriteLine($"Storage after writing data is {storage.Length} bytes");
        }

        [Test]
        public void lookup_paths_for_a_document_id()
        {
            var storage = new MemoryStream();
            var subject = new PageStreamStorage(storage);
            
            var target = Guid.NewGuid();

            subject.BindPath("one"  , target        , out _);
            subject.BindPath("two"  , Guid.NewGuid(), out _);
            subject.BindPath("three", target        , out _);
            subject.BindPath("four" , target        , out _);
            subject.BindPath("five" , Guid.NewGuid(), out _);
            subject.BindPath("six"  , Guid.NewGuid(), out _);

            var list = string.Join(",", subject.GetPathsForDocument(target));
            Assert.That(list, Is.EqualTo("one,three,four"));
        }

        [Test]
        public void search_paths_by_prefix () {
            var storage = new MemoryStream();
            var subject = new PageStreamStorage(storage);
            
            subject.BindPath("find me/one"  , Guid.NewGuid(), out _);
            subject.BindPath("find me/two"  , Guid.NewGuid(), out _);
            subject.BindPath("miss me/three", Guid.NewGuid(), out _);
            subject.BindPath("find me/four" , Guid.NewGuid(), out _);
            subject.BindPath("miss me/five" , Guid.NewGuid(), out _);
            subject.BindPath("miss me/six"  , Guid.NewGuid(), out _);

            var list = string.Join(",", subject.SearchPaths("find me/"));
            Assert.That(list, Is.EqualTo("find me/one,find me/two,find me/four"));
        }
    }
}