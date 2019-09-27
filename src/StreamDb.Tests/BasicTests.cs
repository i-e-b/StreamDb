﻿using System;
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

                var recoveredData = 0;

                var found = subject.Get("images/staff/phil's face", out var dataStream);
                Assert.That(found, Is.True, "Failed to recover first document");
                recoveredData += (int)dataStream.Length;
                Console.WriteLine($"Recovered first document, size = {dataStream.Length / 1024}kb");
                
                found = subject.Get("images/staff/paul's mum", out dataStream);
                Assert.That(found, Is.True, "Failed to recover second document");
                recoveredData += (int)dataStream.Length;
                Console.WriteLine($"Recovered second document, size = {dataStream.Length / 1024}kb");

                Assert.That(recoveredData, Is.EqualTo(trueData), $"Recovered data was a different size to stored originals {trueData} in, {recoveredData} out.");
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
        public void trying_to_read_a_damaged_stream_gives_a_failure_result (){
            // at which point, you'd have a 'recover' method to call


            // Build a db in ram, then write over 1 byte per page
            using (var ms = new MemoryStream())
            {
                var subject = Database.TryConnect(ms);

                Console.WriteLine($"Empty database is {ms.Length / 1024}kb");

                // Write a document
                using (var docStream = MakeTestDocument()){
                    Console.WriteLine($"Writing {docStream.Length / 1024}kb document");
                    subject.WriteDocument("this document will be damaged", docStream);
                }

                // now damage the stream
                for (int i = 0; i < ms.Length; i+= 2000)
                {
                    ms.Seek(i, SeekOrigin.Begin);
                    ms.WriteByte(0);
                }

                // finally, try to read the document back
                Assert.Throws<Exception>(()=>{subject.Get("this document will be damaged", out _);}, "Database did not notice damage");
            }
        }
        
        

        [Test, Explicit("Slow test")]
        public void stress_test_write (){
            using (var doc = MakeTestDocument())
            using (var ms = new MemoryStream())
            {
                var subject = Database.TryConnect(ms);

                Console.WriteLine($"Empty database is {ms.Length / 1024}kb");

                // write lots of documents, and overwrite them a lot of times
                for (int overwrites = 0; overwrites < 10; overwrites++)
                {
                    Console.Write("Writing a 100 document block");

                    for (int i = 0; i < 100; i++)
                    {
                        Console.Write(".");
                        doc.Seek(0, SeekOrigin.Begin);
                        subject.WriteDocument($"testdata-{i}", doc);
                    }

                    Console.WriteLine($"Done. Filled database is {(ms.Length / 1048576.0):#.00}MB");
                }
            }
        }
        
        [Test, Explicit("Slow test")]
        public void stress_test_read (){
            using (var doc = MakeTestDocument())
            using (var ms = new MemoryStream())
            {
                var subject = Database.TryConnect(ms);

                Console.WriteLine("Writing doc");
                doc.Seek(0, SeekOrigin.Begin);
                subject.WriteDocument("test/data-path/doc", doc);


                // Read the same document a load of times
                Console.WriteLine("Reading doc 10'000 times");
                for (int i = 0; i < 5_000; i++)
                {
                    var ok = subject.Get("test/data-path/doc", out _);
                    Assert.That(ok, Is.True);
                    
                    ok = subject.Get($"this document is not here #{i}", out _);
                    Assert.That(ok, Is.False);
                }
                Console.WriteLine("Done");
            }
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
