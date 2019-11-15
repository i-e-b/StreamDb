using System;
using System.IO;
using NUnit.Framework;
using StreamDb.Internal.Support;
using StreamDb.Tests.Helpers;

namespace StreamDb.Tests
{
    [TestFixture]
    public class PersistentReverseTrie {

        [Test]
        public void construction () {
            // Idea:
            // We build a trie (wide, not ternary) using backward links only (as an alternative to a persistent trie)
            // This is our permanent and growable data structure.
            // We then make a separate 'forward links index' as a NON-STORED in-memory cache.
            // This cache gets invalidated on every write, and rebuilt during a read if the invalidation flag is set.
            // (ThreadStatic on cache?)
            //
            // Note: using a normal persistent trie might cause an issue, as it duplicates on prefix reuse.

            var subject = new ReverseTrie<SerialGuid>();

            var val1 = SerialGuid.Wrap(Guid.NewGuid());
            var val2 = SerialGuid.Wrap(Guid.NewGuid());
            var val3 = SerialGuid.Wrap(Guid.NewGuid());
            var val4 = SerialGuid.Wrap(Guid.NewGuid());

            subject.Add("Hello world", val1);
            subject.Add("Hello Dave", val2);
            subject.Add("Jello world", val3);
            subject.Add("Hello worldly goods", val4);

            Console.WriteLine($"\r\nResult:\r\n{subject.DiagnosticDescription()}");
        }

        [Test]
        public void query_exact_path () {
            var subject = new ReverseTrie<SerialGuid>();

            var val1 = SerialGuid.Wrap(Guid.NewGuid());
            var val2 = SerialGuid.Wrap(Guid.NewGuid());
            var val3 = SerialGuid.Wrap(Guid.NewGuid());
            var val4 = SerialGuid.Wrap(Guid.NewGuid());

            subject.Add("Hello world", val1);
            subject.Add("Hello Dave", val2);
            subject.Add("Jello world", val3);
            subject.Add("Hello worldly goods", val4);

            var result1 = subject.Get("Hello Dave");
            var result2 = subject.Get("Hello Sam");

            Assert.That(result1, Is.EqualTo(val2), "Failed to find data to a known path");
            Assert.That(result2, Is.Null, "Was given a result to a path that was not added");
        }

        [Test]
        public void serialisation_and_restoring () {
            var subject = new ReverseTrie<ByteString>();
            var rawLength = 0;

            // Fill it up
            for (int i = 0; i < 100; i++)
            {
                var key = $"Path number {i}";
                var value = $"{i}";

                rawLength += key.Length + value.Length;

                subject.Add(key, ByteString.Wrap(value));
            }

            var frozen = subject.Freeze();
            frozen.Seek(0, SeekOrigin.Begin);

            Console.WriteLine($"Encoding 100 paths in {frozen.Length} bytes. Raw input was {rawLength} bytes");
            Console.WriteLine(frozen.ToHexString());
            subject = null;

            var result = new ReverseTrie<ByteString>();
            frozen.Seek(0, SeekOrigin.Begin);
            result.Defrost(frozen);

            // Check every result
            for (int i = 0; i < 100; i++)
            {
                var key = $"Path number {i}";
                var expected = $"{i}";
                Assert.That(result.Get(key)?.ToString(), Is.EqualTo(expected), $"Lost data at path '{key}'");
            }
        }

        [Test]
        public void deleting_a_value_from_a_path () {
            var subject = new ReverseTrie<SerialGuid>();

            var val1 = SerialGuid.Wrap(Guid.NewGuid());
            var val2 = SerialGuid.Wrap(Guid.NewGuid());

            subject.Add("Deleted: no", val1);
            subject.Add("Deleted: yes", val2);

            subject.Delete("Deleted: yes");
            subject.Delete("Not present"); // ok to try non-paths

            // Get
            Assert.That(subject.Get("Deleted: no"), Is.EqualTo(val1), "Failed to find data to a known path");
            Assert.That(subject.Get("Deleted: yes"), Is.Null, "Should have been removed, but it's still there");

            // Search
            var all = string.Join(",",subject.Search("Deleted"));
            Assert.That(all, Is.EqualTo("Deleted: no"));

            // Look-up
            Assert.That(subject.GetPathsForEntry(val2), Is.Empty, "Value cache was not updated");
            Assert.That(subject.GetPathsForEntry(val1), Is.Not.Empty, "Value cache was destroyed?");
        }

        [Test]
        public void search_with_a_path_prefix () {
            var subject = new ReverseTrie<ByteString>();

            subject.Add("my/path/1", "value1");
            subject.Add("my/path/2", "value2");
            subject.Add("my/other/path", "value3");
            subject.Add("my/other/path/longer", "value4");

            var result = subject.Search("my/pa");

            Assert.That(string.Join(",", result), Is.EqualTo("my/path/1,my/path/2"));
        }

        
        [Test]
        public void can_look_up_paths_by_value_in_live_data () {
            // you can assign the same value to multiple paths
            // this could be quite useful, but I'd like to be able to
            // reverse the process -- see what paths an objects is bound
            // to. Could be a simple set of scans (slow) or a restructuring
            // of the internal data.
            
            var source = new ReverseTrie<ByteString>();

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
            
            var source = new ReverseTrie<ByteString>();

            source.Add("very different", "value0");
            source.Add("my/path/1", "value1");
            source.Add("my/path/2", "value2");
            source.Add("b - very different", "value0");
            source.Add("my/other/path", "value3");
            source.Add("my/other/path/longer", "value4");
            source.Add("another/path/for/3", "value3");
            source.Add("z - very different", "value0");

            var bytes = source.Freeze();
            var reconstituted = new ReverseTrie<ByteString>();
            bytes.Seek(0, SeekOrigin.Begin);
            reconstituted.Defrost(bytes);

            var result = string.Join(", ", reconstituted.GetPathsForEntry("value3"));

            Assert.That(result, Is.EqualTo("my/other/path, another/path/for/3"));
        }
    }
}