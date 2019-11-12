using System;
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

    }
}