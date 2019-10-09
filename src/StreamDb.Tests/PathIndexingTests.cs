using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using StreamDb.Internal.DbStructure;
using StreamDb.Internal.Support;

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
                Console.WriteLine($"Indexing overhead is {(ms.Length / (float)totalBytes):#.0} times");
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
        public void survives_serialisation () {

            var source = new PathIndex<ByteString>();

            source.Add("my/path/1", "value1");
            source.Add("my/path/2", "value2");
            source.Add("my/other/path", "value3");
            source.Add("my/other/path/longer", "value4");

        
            var bytes = source.Freeze();

            bytes.Seek(0, SeekOrigin.Begin);

            Console.WriteLine(bytes.ToHexString());

            var result = new PathIndex<ByteString>();
            bytes.Seek(0, SeekOrigin.Begin);
            result.Defrost(bytes);


            Assert.That((string)result.Get("my/path/1"), Is.EqualTo("value1"));
            Assert.That((string)result.Get("my/path/2"), Is.EqualTo("value2"));
            Assert.That((string)result.Get("my/other/path"), Is.EqualTo("value3"));
            Assert.That((string)result.Get("my/other/path/longer"), Is.EqualTo("value4"));
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

        [Test]
        public void can_incrementally_extend_the_serialised_form () {

            // 1. Write a few paths, and serialise to an array
            // 2. Write a few more path, serialise to another array
            // 3. Make a third array, from the head of the first and the tail of the second
            // 4. Deserialise from the 3rd array, check it is fully functional.

            // NOTE: this does not cover changes to pre-existing paths!

            var source = new PathIndex<ByteString>();

            source.Add("my/path/1", "value1");
            source.Add("my/path/2", "value2");
            source.Add("my/other/path", "value3");
            source.Add("my/other/path/longer", "value4");

            // get originals
            var bytes1 = source.Freeze();
            Console.WriteLine($"First len = {bytes1.Length}");

            source.Add("my/path/3", "value5");
            source.Add("my/path/4", "value6");
            source.Add("my/third/path", "value7");
            source.Add("my/other/path/longer-still", "PPPPPPPPP"); // F0090000005050505050...

            // get extended
            var bytes2 = source.Freeze();
            Console.WriteLine($"Second len = {bytes2.Length}");
            Assert.That(bytes2.Length, Is.GreaterThan(bytes1.Length), "Adding entries did not change the serialised size");

            // illustrate data
            //Console.WriteLine(bytes1.ToHexString());
            //Console.WriteLine(bytes2.ToHexString());

            // make a new one with only the added bytes
            var concat = new MemoryStream();

            bytes1.Seek(0, SeekOrigin.Begin);
            bytes1.CopyTo(concat);

            bytes2.Seek(bytes1.Length - 1, SeekOrigin.Begin);
            bytes2.CopyTo(concat);


            var result = new PathIndex<ByteString>();
            concat.Seek(0, SeekOrigin.Begin);
            result.Defrost(concat);

            Assert.That((string)result.Get("my/path/1"), Is.EqualTo("value1"));
            Assert.That((string)result.Get("my/path/2"), Is.EqualTo("value2"));
            Assert.That((string)result.Get("my/other/path"), Is.EqualTo("value3"));
            Assert.That((string)result.Get("my/other/path/longer"), Is.EqualTo("value4"));
            Assert.That((string)result.Get("my/path/3"), Is.EqualTo("value5"));
            Assert.That((string)result.Get("my/path/4"), Is.EqualTo("value6"));
            Assert.That((string)result.Get("my/third/path"), Is.EqualTo("value7"));
            Assert.That((string)result.Get("my/other/path/longer-still"), Is.EqualTo("PPPPPPPPP"));
        }

        [Test]
        public void can_lookup_populated_paths_given_a_prefix () {
            var source = new PathIndex<ByteString>();

            source.Add("what/path/1", "value1");
            source.Add("my/path/1", "value1");
            source.Add("my/path/2", "value2");
            source.Add("my/other/path", "value3");
            source.Add("my/path/3", "value4");

            Console.WriteLine(source.DiagnosticString());

            var result = string.Join(", ", source.Search("my/path"));
            Assert.That(result, Is.EqualTo("my/path/1, my/path/2, my/path/3"));
            
            result = string.Join(", ", source.Search("what"));
            Assert.That(result, Is.EqualTo("what/path/1"));
        }

        [Test]
        public void can_look_up_paths_by_value_in_live_data () {
            // you can assign the same value to multiple paths
            // this could be quite useful, but I'd like to be able to
            // reverse the process -- see what paths an objects is bound
            // to. Could be a simple set of scans (slow) or a restructuring
            // of the internal data.
            
            var source = new PathIndex<ByteString>();

            source.Add("very different", "value0");
            source.Add("my/path/1", "value1");
            source.Add("my/path/2", "value2");
            source.Add("b - very different", "value0");
            source.Add("my/other/path", "value3");
            source.Add("my/other/path/longer", "value4");
            source.Add("another/path/for/3", "value3");
            source.Add("z - very different", "value0");

            var result = string.Join(", ", source.GetPathsForEntry("value3"));

            Assert.That(result, Is.EqualTo("my/other/path, another/path/for/3"));
        }

        [Test]
        public void can_look_up_paths_by_value_from_serialised_data () {
            // you can assign the same value to multiple paths
            // this could be quite useful, but I'd like to be able to
            // reverse the process -- see what paths an objects is bound
            // to. Could be a simple set of scans (slow) or a restructuring
            // of the internal data.
            
            var source = new PathIndex<ByteString>();

            source.Add("very different", "value0");
            source.Add("my/path/1", "value1");
            source.Add("my/path/2", "value2");
            source.Add("b - very different", "value0");
            source.Add("my/other/path", "value3");
            source.Add("my/other/path/longer", "value4");
            source.Add("another/path/for/3", "value3");
            source.Add("z - very different", "value0");

            var bytes = source.Freeze();
            var reconstituted = new PathIndex<ByteString>();
            bytes.Seek(0, SeekOrigin.Begin);
            reconstituted.Defrost(bytes);

            var result = string.Join(", ", reconstituted.GetPathsForEntry("value3"));

            Assert.That(result, Is.EqualTo("my/other/path, another/path/for/3"));
        }

        [Test]
        public void can_remove_a_path () {
            var source = new PathIndex<ByteString>();

            source.Add("my/path/1", "value0");
            source.Add("my/path/dead", "value0");
            source.Add("my/path/2", "value0");

            source.Delete("my/path/dead");
            
            var result = string.Join(", ", source.GetPathsForEntry("value0"));

            Assert.That(result, Is.EqualTo("my/path/1, my/path/2"));
        }

        [Test]
        public void path_removal_survives_incremental_serialisation () {
            
            // 1. Write a few paths, and serialise to an array
            // 2. Write a few more path, serialise to another array
            // 3. Make a third array, from the head of the first and the tail of the second
            // 4. Deserialise from the 3rd array, check it is fully functional.

            // NOTE: this does not cover changes to pre-existing paths!

            var source = new PathIndex<ByteString>();

            source.Add("my/path/1", "value1");
            source.Add("my/path/2", "value2");
            source.Add("my/other/path", "value3");
            source.Add("my/other/path/longer", "value4");

            // get originals
            var bytes1 = source.Freeze();
            Console.WriteLine($"First len = {bytes1.Length}");

            source.Add("my/path/3", "value5");
            source.Add("my/path/4", "value6");
            source.Delete("my/path/1");
            source.Delete("my/path/2");

            // get extended
            var bytes2 = source.Freeze();
            Console.WriteLine($"Second len = {bytes2.Length}");
            Assert.That(bytes2.Length, Is.GreaterThan(bytes1.Length), "Adding entries did not change the serialised size");

            // make a new one with only the added bytes
            var concat = new MemoryStream();

            bytes1.Seek(0, SeekOrigin.Begin);
            bytes1.CopyTo(concat);

            bytes2.Seek(bytes1.Length - 1, SeekOrigin.Begin);
            bytes2.CopyTo(concat);
            
            // illustrate data
            bytes1.Seek(0,SeekOrigin.Begin);
            bytes2.Seek(0,SeekOrigin.Begin);
            Console.WriteLine(bytes1.ToHexString());
            Console.WriteLine(bytes2.ToHexString());


            var result = new PathIndex<ByteString>();
            concat.Seek(0, SeekOrigin.Begin);
            result.Defrost(concat);

            Assert.That((string)result.Get("my/path/1"), Is.Null);
            Assert.That((string)result.Get("my/path/2"), Is.Null);
            Assert.That((string)result.Get("my/other/path"), Is.EqualTo("value3"));
            Assert.That((string)result.Get("my/other/path/longer"), Is.EqualTo("value4"));
            Assert.That((string)result.Get("my/path/3"), Is.EqualTo("value5"));
            Assert.That((string)result.Get("my/path/4"), Is.EqualTo("value6"));
        }


        [Test]
        public void out_of_order_inserts_can_be_recovered ()
        {
            // build up a cluster of variants at the end of a common chain
            var subject = new PathIndex<ByteString>();
            var ooo = Enumerable.Range(0, 100).ToList().Shuffle() ?? throw new Exception("Setup failed");

            // insert out of order
            foreach (var i in ooo)
            {
                subject.Add($"test/path/{i}", $"{i}");
            }

            // read back
            for (int i = 0; i < 100; i++)
            {
                var result = subject.Get($"test/path/{i}");
                Assert.That((string)result, Is.EqualTo($"{i}"), $"Failed to read {i}");
            }
        }

    }
}