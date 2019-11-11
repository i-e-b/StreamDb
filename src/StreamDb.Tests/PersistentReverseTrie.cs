using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using StreamDb.Internal.Support;
using StreamDb.Internal.Search;

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

        // IEnumerable<string> GetPathsForEntry(T value)
    }

    public class ReverseTrie<TValue> : IStreamSerialisable where TValue : class, IStreamSerialisable, new()
    {
        public class RTNode:PartiallyOrdered {
            private static readonly object _lock = new object();
            public readonly char Value;
            public readonly int Parent;

            /// <summary>This is set during storage to help lookups </summary>
            public int SelfIndex;

            /// <summary>
            /// Optional data stored with this node
            /// </summary>
            public TValue Data;

            public RTNode(char value, int parent) {
                Value = value;
                Parent = parent;
            }

            public static int AddNewNode(char value, int parent, List<RTNode> target) {
                lock (_lock)
                {
                    var idx = target.Count;
                    target.Add(new RTNode(value, parent) { SelfIndex = idx });
                    return idx;
                }
            }

            /// <inheritdoc />
            public override int CompareTo(object obj) {
                if (obj == null || !(obj is RTNode node)) { return -1; }
                if (node.Parent != Parent) return Parent.CompareTo(node.Parent);
                return Value.CompareTo(node.Value);
            }

            /// <inheritdoc />
            public override int GetHashCode() { return Value.GetHashCode() ^ Parent.GetHashCode(); }
        }

        private const char RootValue = '\0'; // all strings point back to a single common root, at index zero.
        private const int RootParent = -1;

        /// <summary>
        /// This is the core list used for storage, and produces indexes.
        /// This is the only data that is serialised.
        /// </summary>
        private readonly List<RTNode> _store;

        /// <summary>
        /// (Parent Index -> Char Value -> Child Index);
        /// This is the 'forward pointing cache' we use during construction and querying
        /// </summary>
        private readonly Map<int, Map<char, int>> _fwdCache;

        /// <summary>
        /// Node-to-Path mapping, for reverse look-ups. Values are entries in the `_store` list, at the end of the path.
        /// </summary>
        private readonly Dictionary<TValue, HashSet<int>> _valueCache;

        public ReverseTrie()
        {
            _store = new List<RTNode>();
            _fwdCache = new Map<int, Map<char, int>>(() => new Map<char, int>());
            _valueCache = new Dictionary<TValue, HashSet<int>>();

            RTNode.AddNewNode(RootValue, RootParent, _store);
        }

        /// <summary>
        /// Add a path/value pair to the trie.
        /// Value can be null. If an existing value was present, it is returned.
        /// </summary>
        /// <param name="path">Complete path to use as a key to the value</param>
        /// <param name="value">Value to be stored on this path</param>
        public TValue Add(string path, TValue value)
        {
            var q = new Queue<char>(path);
            var currentNode = 0; // root is always at zero

            while (q.Count > 0)
            {
                var c = q.Dequeue();

                // Find link from current to next (will continually fail when we're writing. This could be optimised)
                var next = NextNode(currentNode, c);
                if (next > 0) {
                    currentNode = next;
                    continue;
                }

                // Not found. Add a new node linked back.
                currentNode = LinkNewNode(currentNode, c);
            }

            var old = _store[currentNode].Data;
            _store[currentNode].Data = value;
            AddToValueCache(currentNode, value);
            return old;
        }
        
        /// <summary>
        /// Read the value stored on the given path.
        /// If no data is stored, the default value is returned
        /// </summary>
        public TValue Get(string path)
        {
            if (!TryFindNodeIndex(path, out var currentNode)) return default;
            return _store[currentNode].Data;
        }

        /// <summary>
        /// Return all known paths that start with the given prefix and contain a value
        /// </summary>
        public IEnumerable<string> Search(string prefix)
        {
            if (!TryFindNodeIndex(prefix, out var currentNode)) yield break;

            // now recurse down all paths from here
            foreach (var nextChar in _fwdCache[currentNode].Keys())
            {
                var child = _fwdCache[currentNode][nextChar];
                foreach (var str in RecursiveSearch(child)) {
                    yield return str;
                }
            }    
        }

        /// <summary>
        /// List all paths currently bound to the given value
        /// </summary>
        public IEnumerable<string> GetPathsForEntry(TValue value) {
            if (value == null) yield break;
            if (!_valueCache.ContainsKey(value)) yield break;

            foreach (var index in _valueCache[value])
            {
                yield return TraceNodePath(index);
            }
        }

        /// <summary>
        /// Delete the value at a path if it exists. If the path doesn't exist or has no value, this command is ignored.
        /// </summary>
        public void Delete(string exactPath)
        {
            if (!TryFindNodeIndex(exactPath, out var currentNode)) return;
            var old = _store[currentNode].Data;
            _store[currentNode].Data = default;
            if (_valueCache.ContainsKey(old)) {
                _valueCache[old].Remove(currentNode);
            }
        }

        /// <inheritdoc />
        public Stream Freeze()
        {
            // We only store the reverse list. The forward cache is always rebuilt.
            var ms = new MemoryStream();
            var dest = new BitwiseStreamWrapper(ms, 1);


            foreach (var node in _store)
            {
                if (node.SelfIndex==0) continue; // don't store root

                EncodeValue((uint)node.Parent, dest);
                EncodeValue(node.Value, dest);

                if (node.Data == null) {
                    EncodeValue(0, dest);
                } else {
                    var raw = node.Data.Freeze();

                    EncodeValue((uint) raw.Length, dest);
                    dest.Flush();
                    raw.CopyTo(ms);
                }
            }

            dest.Flush();
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

        /// <inheritdoc />
        public void Defrost(Stream source)
        {
            var src = new BitwiseStreamWrapper(source, 1);

            // reset to starting condition
            _store.Clear();
            _fwdCache.Clear();
            RTNode.AddNewNode(RootValue, RootParent, _store);

            var allOk = true;
            while (!src.IsEmpty()) {
                if (!TryDecodeValue(src, out var parent)) { break; }
                if (!TryDecodeValue(src, out var value)) throw new Exception("Invalid structure: Entry truncated at child");
                if (!TryDecodeValue(src, out var dataLength)) throw new Exception("Invalid structure: Entry truncated at data");

                if (parent > _store.Count) throw new Exception("Invalid structure: found a parent forward of child");

                var newIdx = RTNode.AddNewNode((char)value, (int)parent, _store);
                if (newIdx <= parent) throw new Exception("Invalid structure: found a forward pointer");


                _fwdCache[(int)parent][(char)value] = newIdx;

                if (dataLength > 0) {
                    var data = new TValue();
                    data.Defrost(new Substream(source, (int)dataLength));
                    _store[newIdx].Data = data;
                    AddToValueCache(newIdx, data);
                }
            }

            Console.WriteLine($"Defrost OK: {allOk}");
        }

        /// <summary>
        /// Provide a human readable string of the storage list. Does not include the forward cache
        /// </summary>
        public string DiagnosticDescription()
        {
            var sb = new StringBuilder();

            foreach (var node in _store)
            {
                if (node.SelfIndex==0) {
                    sb.Append("Root[0]");
                }
                else if (node.Data == null){
                    sb.Append($" | {node.Value} [{node.SelfIndex}]->{node.Parent}");
                }
                else {
                    sb.Append($" | +{node.Value}+ [{node.SelfIndex}]->{node.Parent}");
                }
            }

            return sb.ToString();
        }


        private void AddToValueCache(int newIdx, TValue data)
        {
            if (!_valueCache.ContainsKey(data)) { _valueCache.Add(data, new HashSet<int>()); }
            _valueCache[data].Add(newIdx);
        }

        private IEnumerable<string> RecursiveSearch(int nodeIdx)
        {
            var node = _store[nodeIdx];
            if (node.Data != null) {
                yield return TraceNodePath(nodeIdx);
            }
            
            foreach (var nextChar in _fwdCache[nodeIdx].Keys())
            {
                var child = _fwdCache[nodeIdx][nextChar];
                foreach (var str in RecursiveSearch(child)) {
                    yield return str;
                }
            }    
        }

        private string TraceNodePath(int nodeIdx)
        {
            // Trace from the node back to root, build a string
            var stack = new Stack<char>();
            while (nodeIdx > 0) {
                stack.Push(_store[nodeIdx].Value);
                nodeIdx = _store[nodeIdx].Parent;
            }
            var sb = new StringBuilder(stack.Count);
            while (stack.Count > 0) sb.Append(stack.Pop());
            return sb.ToString();
        }

        private bool TryFindNodeIndex(string path, out int currentNode)
        {
            var q = new Queue<char>(path);
            currentNode = 0;

            while (q.Count > 0)
            {
                var c = q.Dequeue();

                var next = NextNode(currentNode, c);
                if (next <= 0) return false;

                currentNode = next;
            }

            return true;
        }

        private int LinkNewNode(int currentNode, char c)
        {
            var idx = RTNode.AddNewNode(c, currentNode, _store);
            
            _fwdCache[currentNode][c] = idx;
            return idx;
        }

        private int NextNode(int currentNode, char c)
        {
            var map = _fwdCache[currentNode];
            if (!map.Contains(c)) return -1;
            return map[c];
        }
        
        /// <summary>
        /// Read a value previously written with `EncodeValue`
        /// </summary>
        private static bool TryDecodeValue(BitwiseStreamWrapper src, out uint value)
        {
            value = 0;
            var ok = src.TryReadBit(out var b);
            if (!ok) return false;

            if (b == 0) { // one byte (7 bit data)
                for (int i = 0; i < 7; i++) {
                    value |= (uint)(src.ReadBit() << i);
                }
                return true;
            }
            
            ok = src.TryReadBit(out b);
            if (!ok) return false;
            if (b == 0) { // two byte (14 bits data)
                for (int i = 0; i < 14; i++) {
                    value |= (uint)(src.ReadBit() << i);
                }
                value += 127;
                return true;
            }
            
            //3 bytes (22 bit data)
            for (int i = 0; i < 22; i++) {
                value |= (uint)(src.ReadBit() << i);
            }
            value += 16384 + 127;
            return true;
        }

        /// <summary>
        /// Compact number encoding that maintains byte alignment
        /// </summary>
        private static void EncodeValue(uint value, BitwiseStreamWrapper dest)
        {
            if (value < 127) { // one byte (7 bits data)
                dest.WriteBit(0);
                for (int i = 0; i < 7; i++) {
                    dest.WriteBit((int) ((value >> i) & 1));
                }
                return;
            }

            value -= 127;

            if (value < 16384) { // two bytes (14 bits data)
                dest.WriteBit(1);
                dest.WriteBit(0);
                for (int i = 0; i < 14; i++) {
                    dest.WriteBit((int) ((value >> i) & 1));
                }
                return;
            }

            value -= 16384;

            // Otherwise 3 bytes (22 bit data)
            dest.WriteBit(1);
            dest.WriteBit(1);
            for (int i = 0; i < 22; i++)
            {
                dest.WriteBit((int)((value >> i) & 1));
            }
        }

    }
}