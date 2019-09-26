using System;
using System.IO;
using NUnit.Framework;
// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class BasicTests
    {
        [Test]
        public void can_create_a_new_blank_database_in_a_stream_and_use_it (){
            using (var ms = new MemoryStream())
            {
                var subject = Database.TryConnect(ms);

                Console.WriteLine($"Empty database is {ms.Length / 1024}kb");
                var trueData = 0L;

                using (var docStream = MakeTestDocument()){
                    Console.WriteLine($"Writing {docStream.Length / 1024}kb document");
                    subject.WriteDocument("images/staff/phil's face", docStream);
                    trueData += docStream.Length;
                }
                
                using (var docStream = MakeTestDocument()){
                    Console.WriteLine($"Writing {docStream.Length / 1024}kb document");
                    subject.WriteDocument("images/staff/paul's mum", docStream);
                    trueData += docStream.Length;
                }
                
                Assert.That(ms.Length, Is.GreaterThan(0), "Stream was not populated");
                Console.WriteLine($"Filled database is {ms.Length / 1024}kb, with document data of {trueData / 1024}kb");

                var found = subject.Get("images/staff/phil's face", out var dataStream);
                Assert.That(found, Is.True, "Failed to recover first document");
                Console.WriteLine($"Recovered first document, size = {dataStream.Length / 1024}kb");
                
                found = subject.Get("images/staff/paul's mum", out dataStream);
                Assert.That(found, Is.True, "Failed to recover second document");
                Console.WriteLine($"Recovered second document, size = {dataStream.Length / 1024}kb");
            }
        }

        [Test]
        public void can_create_a_database_with_a_file_stream ()
        {
            using (var fs = File.Open(@"C:\Temp\StreamDBTest.dat", FileMode.Create, FileAccess.ReadWrite, FileShare.None))
            using (var db = Database.TryConnect(fs))
            {

                // write some documents
                for (int i = 0; i < 10; i++)
                {
                    using (var docStream = MakeTestDocument())
                    {
                        Console.WriteLine($"Writing {docStream.Length / 1024}kb document");
                        db.WriteDocument($"testdata-{i}", docStream);
                    }
                }
                
                // Now overwrite some of the documents...
                for (int i = 3; i < 7; i++)
                {
                    using (var docStream = MakeTestDocument())
                    {
                        Console.WriteLine($"Writing {docStream.Length / 1024}kb document");
                        db.WriteDocument($"testdata-{i}", docStream);
                    }
                }

                Console.WriteLine("Database file is populated, and can be used by other tests");
            }
        }

        [Test]
        public void z_can_open_an_existing_database_from_a_file_stream()
        {
            using (var fs = File.Open(@"C:\Temp\StreamDBTest.dat", FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            using (var db = Database.TryConnect(fs))
            {

                for (int i = 0; i < 10; i++)
                {
                    var found = db.Get($"testdata-{i}", out var docStream);
                    Assert.That(found, Is.True, $"Lost document #{i}");
                    Console.WriteLine($"Read {docStream.Length / 1024}kb document at 'testdata-{i}'");
                }
            }
        }

        [Test]
        public void database_can_be_accessed_with_a_readonly_stream ()
        {
            // We expect write operations to fail, but should be able to full access all data.
            using (var fs = File.Open(@"C:\Temp\StreamDBTest.dat", FileMode.Open, FileAccess.Read, FileShare.None))
            using (var db = Database.TryConnect(fs))
            {

                for (int i = 0; i < 10; i++)
                {
                    var found = db.Get($"testdata-{i}", out var docStream);
                    Assert.That(found, Is.True, $"Lost document #{i}");
                    Console.WriteLine($"Writing {docStream.Length / 1024}kb document");
                }
            }
        }

        [Test]
        public void trying_to_open_a_damaged_stream_gives_a_failure_result (){
            // at which point, you'd have a 'recover' method to call
            Assert.Fail("NYI");
        }
        

        private static Stream MakeTestDocument()
        {
            var ms = new MemoryStream();
            var rnd= new Random();
            var buf = new byte[1024];
            for (int i = 0; i < rnd.Next(102, 1024); i++)
            {
                rnd.NextBytes(buf);
                ms.Write(buf, 0, buf.Length);
            }
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

    }
}
