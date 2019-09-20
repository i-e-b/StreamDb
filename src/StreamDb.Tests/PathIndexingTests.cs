using System;
using System.IO;
using NUnit.Framework;

namespace StreamDb.Tests
{
    [TestFixture]
    public class PathIndexingTests
    {
        [Test]
        public void can_add_keys_to_tree ()
        {
            var subject = new PathIndex<ByteString>();

            subject.Add("my/path/1", "value1");
            subject.Add("my/other/path", "value2");

            Console.WriteLine(subject.DiagnosticString());
        }

        [Test]
        public void stress_test () {
            var subject = new PathIndex<ByteString>();

            subject.Add("start", "start value");

            long totalBytes = 0;
            for (int i = 0; i < 1000; i++)
            {
                var newKey = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
                var newValue = Convert.ToBase64String(Guid.NewGuid().ToByteArray());

                totalBytes += newKey.Length;
                totalBytes += newValue.Length;

                subject.Add(newKey, newValue);
            }

            subject.Add("end", "end value");
            
            Assert.That((string)subject.Get("start"), Is.EqualTo("start value"));
            Assert.That((string)subject.Get("end"), Is.EqualTo("end value"));

            
            using (var ms = new MemoryStream()) {
                subject.WriteTo(ms);
                Console.WriteLine($"Produced {totalBytes} bytes");
                Console.WriteLine($"Stored {ms.Length} bytes");
            }

            Console.WriteLine(subject.DiagnosticString());
        }

        [Test]
        public void can_query_keys_for_values ()
        {
            var subject = new PathIndex<ByteString>();

            subject.Add("my/path/1", "value1");
            subject.Add("my/path/2", "value2");
            subject.Add("my/other/path", "value3");
            subject.Add("my/other/path/longer", "value4");

            var r1 = subject.Get("my/path/2");
            var r2 = subject.Get("my/other/path");
            var r3 = subject.Get("not/here");

            Assert.That((string)r1, Is.EqualTo("value2"));
            Assert.That((string)r2, Is.EqualTo("value3"));
            Assert.That(r3, Is.Null);
        }
        
        [Test]
        public void can_query_keys_using_a_partial_key ()
        {
            var subject = new PathIndex<ByteString>();

            subject.Add("my/path/1", "value1");
            subject.Add("my/path/2", "value2");
            subject.Add("my/other/path", "value3");
            subject.Add("my/other/path/longer", "value4");

            var result = subject.Search("my/pa");

            Assert.That(string.Join(",", result), Is.EqualTo("my/path/1,my/path/2"));
        }

        [Test]
        public void can_remove_values_from_keys () {
            // Note -- we don't actually remove the key, just the value
            // This is the same as setting the value to null.

            var subject = new PathIndex<ByteString>();

            subject.Add("my/path/1", "value1");
            subject.Add("my/path/2", "value2");
            subject.Add("my/other/path", "value3");
            subject.Add("my/other/path/longer", "value4");

            var r1 = subject.Get("my/path/2");
            Assert.That((string)r1, Is.EqualTo("value2"));


            subject.Delete("my/path/2");

            var r2 = subject.Get("my/other/path");
            var r3 = subject.Get("my/path/1");
            var r4 = subject.Get("my/path/2");

            Assert.That((string)r2, Is.EqualTo("value3"));
            Assert.That((string)r3, Is.EqualTo("value1"));
            Assert.That((string)r4, Is.Null);
        }

        [Test]
        public void can_output_to_a_stream ()
        {
            var subject = new PathIndex<ByteString>();

            subject.Add("my/path/1", "value1");
            subject.Add("my/path/2", "value2");
            subject.Add("my/other/path", "value3");
            subject.Add("my/other/path/longer", "value4");

            using (var ms = new MemoryStream()) {
                subject.WriteTo(ms);
                Assert.That(ms.Length, Is.GreaterThan(10));

                Console.WriteLine($"Wrote {ms.Length} bytes");
            }
        }

        [Test]
        public void can_read_from_a_stream () 
        {
            var source = new PathIndex<ByteString>();

            source.Add("my/path/1", "value1");
            source.Add("my/path/2", "value2");
            source.Add("my/other/path", "value3");
            source.Add("my/other/path/longer", "value4");

            using (var ms = new MemoryStream()) {
                source.WriteTo(ms);
                ms.Seek(0, SeekOrigin.Begin);
                var target = PathIndex<ByteString>.ReadFrom(ms);

                Assert.That((string)target.Get("my/path/1"), Is.EqualTo("value1"));
                Assert.That((string)target.Get("my/path/2"), Is.EqualTo("value2"));
                Assert.That((string)target.Get("my/other/path"), Is.EqualTo("value3"));
                Assert.That((string)target.Get("my/other/path/longer"), Is.EqualTo("value4"));
            }
        }

        [Test]
        public void indexing_guid_values () {
            var source = new PathIndex<SerialGuid>();

            var guid1 = Guid.NewGuid();
            var guid2 = Guid.NewGuid();
            source.Add("/etc/init.d/01-system.sh", guid1);
            source.Add("/etc/init.d/02-user.sh", guid2);
            
            Assert.That((Guid)source.Get("/etc/init.d/01-system.sh"), Is.EqualTo(guid1));
            Assert.That((Guid)source.Get("/etc/init.d/02-user.sh"), Is.EqualTo(guid2));
            Assert.That(source.Get("/etc/init.d/03-custom.sh"), Is.EqualTo(null));
        }
    }
}