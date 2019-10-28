using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using StreamDb.Internal.Support;

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
                var subject = Database_OLD.TryConnect(ms);

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
            using (var db = Database_OLD.TryConnect(fs))
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
            using (var db = Database_OLD.TryConnect(fs))
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
            using (var db = Database_OLD.TryConnect(fs))
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
                var subject = Database_OLD.TryConnect(ms);

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
                var ex = Assert.Throws<Exception>(()=>{subject.Get("this document will be damaged", out _);}, "Database did not notice damage");
                Assert.That(ex.Message, Contains.Substring("Data integrity check failed"));
            }
        }
        
        [Test]
        public void lookup_the_paths_for_a_document_id () {
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

                var docId = subject.WriteDocument("original/path", MakeTestDocument());
                subject.BindToPath(docId, "new/path/same/document");
                
                subject.WriteDocument("different", MakeTestDocument()); // should not be found

                var found = string.Join(", ", subject.ListPaths(docId));
                Assert.That(found, Is.EqualTo("original/path, new/path/same/document"));
            }
        }
        
        [Test]
        public void can_bind_a_large_number_of_paths () {
            using (var ms = new MemoryStream())
            {
                const int rounds = 250;

                var subject = Database_OLD.TryConnect(ms);

                var docId = subject.WriteDocument("original/path", MakeTestDocument());

                var preLen = ms.Length;

                // Bind a load of paths
                for (int i = 0; i < rounds; i++)
                {
                    subject.BindToPath(docId, $"new/path/number_{i}");
                }

                var postLen = ms.Length;

                Console.WriteLine($"Storage for {rounds} similar paths took {(postLen - preLen)/1024}KB. Total DB size = {postLen/1024}KB");

                // read back
                var found1 = subject.ListPaths(docId).ToList();
                Assert.That(found1.Count, Is.EqualTo(rounds+1), "Paths were not recorded to cache");

                // serialise
                ms.Rewind();
                var raw = ms.ToArray();
                var result = Database_OLD.TryConnect(new MemoryStream(raw));

                // read back
                var found = result.ListPaths(docId).ToList();
                Assert.That(found.Count, Is.EqualTo(rounds+1), "Paths were not recorded to storage");
            }
        }

        [Test]
        public void search_for_paths_with_a_path_prefix () {
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

                // spam a few paths
                subject.WriteDocument("Some may say that this is not a path", MakeTestDocument());
                subject.WriteDocument("But the route you take is your own", MakeTestDocument());
                subject.WriteDocument("test result uno", MakeTestDocument());
                subject.WriteDocument("test result dos", MakeTestDocument());
                subject.WriteDocument("{This is really all for padding}", MakeTestDocument());

                var result = subject.Search("test result");

                var found = string.Join(", ", result);
                Assert.That(found, Is.EqualTo("test result uno, test result dos"));
            }
        }

        [Test]
        public void removing_a_document_removes_it_from_all_paths (){
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

                var docId = subject.WriteDocument("original/path", MakeTestDocument());
                subject.BindToPath(docId, "new/path/same/document");
                
                subject.Delete(docId);

                var found = subject.ListPaths(docId).ToList();
                Assert.That(found, Is.Empty);
            }
        }

        [Test]
        public void unbinding_a_document_from_a_path_does_not_remove_the_document_or_other_paths (){
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

                var docId = subject.WriteDocument("original/path", MakeTestDocument());
                subject.BindToPath(docId, "new/path/same/document");
                
                subject.UnbindPath(docId, "original/path");

                var found = string.Join(", ", subject.ListPaths(docId));
                Assert.That(found, Is.EqualTo("new/path/same/document"));
            }
        }

        [Test]
        public void unbinding_the_last_path_for_a_document_does_not_delete_it () {
            using (var ms = new MemoryStream())
            {
                var testDoc = MakeTestDocument();
                var subject = Database_OLD.TryConnect(ms);

                var docId = subject.WriteDocument("original/path", testDoc);
                subject.UnbindPath(docId, "original/path");

                // check nothing is bound
                var found = subject.ListPaths(docId).ToList();
                Assert.That(found, Is.Empty, "Path were still bound, but should have been empty");

                // bind a new path
                subject.BindToPath(docId, "new/path");

                // check we can read it
                var ok = subject.Get("new/path", out var data);
                Assert.That(ok, Is.True, "Get failed");

                // check data is correct
                var original = testDoc.ToHexString();
                var result = data.ToHexString();
                Assert.That(result, Is.EqualTo(original));
            }
        }
        
        [Test, Explicit("Slow test")]
        public void stress_test_overwrite (){
            using (var doc = MakeTestDocument())
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

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
        public void stress_test_unique_write (){
            using (var doc = MakeTestDocument())
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

                Console.WriteLine($"Empty database is {ms.Length / 1024}kb");

                // write lots of documents, all in new document chains
                Console.Write("Writing 1000 documents");

                for (int i = 0; i < 1000; i++)
                {
                    Console.Write(".");
                    doc.Seek(0, SeekOrigin.Begin);
                    subject.WriteDocument($"testdata-{i}", doc);
                }

                Console.WriteLine($"Done. Filled database is {(ms.Length / 1048576.0):#.00}MB");
            }
        }
        
        [Test, Explicit("Slow test")]
        public void stress_test_read (){
            using (var doc = MakeTestDocument())
            using (var ms = new MemoryStream())
            {
                var subject = Database_OLD.TryConnect(ms);

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
