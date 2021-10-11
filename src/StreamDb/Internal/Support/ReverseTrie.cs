using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using JetBrains.Annotations;
using StreamDb.Internal.Search;

namespace StreamDb.Internal.Support
{
    public class ReverseTrie<TValue> : IStreamSerialisable where TValue : class, IStreamSerialisable, new()
    {
        public class RtNode : PartiallyOrdered {
            public readonly char Value;
            public readonly int Parent;

            /// <summary>This is set during storage to help lookups </summary>
            public int SelfIndex;

            /// <summary>
            /// Optional data stored with this node
            /// </summary>
            public TValue? Data;

            public RtNode(char value, int parent) {
                Value = value;
                Parent = parent;
            }

            public static int AddNewNode(char value, int parent, List<RtNode> target) {
                if (target == null) throw new Exception("Can't add a node to a null target");
                lock (target)
                {
                    var idx = target.Count;
                    target.Add(new RtNode(value, parent) { SelfIndex = idx });
                    return idx;
                }
            }

            /// <inheritdoc />
            public override int CompareTo(object? obj) {
                if (obj == null || !(obj is RtNode node)) { return -1; }
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
        [NotNull, ItemNotNull]private readonly List<RtNode> _store;

        /// <summary>
        /// (Parent Index -> Char Value -> Child Index);
        /// This is the 'forward pointing cache' we use during construction and querying
        /// </summary>
        [NotNull]private readonly Map<int, Map<char, int>> _fwdCache;

        /// <summary>
        /// Node-to-Path mapping, for reverse look-ups. Values are entries in the `_store` list, at the end of the path.
        /// </summary>
        [NotNull]private readonly Dictionary<TValue, HashSet<int>> _valueCache;

        public ReverseTrie()
        {
            _store = new List<RtNode>();
            _fwdCache = new Map<int, Map<char, int>>(() => new Map<char, int>());
            _valueCache = new Dictionary<TValue, HashSet<int>>();

            RtNode.AddNewNode(RootValue, RootParent, _store);
        }

        /// <summary>
        /// Add a path/value pair to the trie.
        /// Value can not be null. If an existing value was present, it is returned.
        /// </summary>
        /// <param name="path">Complete path to use as a key to the value</param>
        /// <param name="value">Value to be stored on this path</param>
        public TValue? Add(string path, TValue? value)
        {
            if (value == null) throw new Exception("Value must not be null");
            if (string.IsNullOrEmpty(path)) throw new Exception("Path must not be null or empty");
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

            if (_store[currentNode] == null) throw new Exception("Internal logic error in ReverseTrie.Add()");
            var old = _store[currentNode]!.Data;
            _store[currentNode]!.Data = value;
            AddToValueCache(currentNode, value);
            return old;
        }
        
        /// <summary>
        /// Read the value stored on the given path.
        /// If no data is stored, the default value is returned
        /// </summary>
        public TValue? Get(string path)
        {
            if (string.IsNullOrEmpty(path)) throw new Exception("Path must not be null or empty");
            if (!TryFindNodeIndex(path, out var currentNode)) return default;
            if (_store[currentNode] == null) throw new Exception("Internal logic error in ReverseTrie.Get()");
            return _store[currentNode]!.Data;
        }

        /// <summary>
        /// Return all known paths that start with the given prefix and contain a value
        /// </summary>
        [NotNull]public IEnumerable<string> Search(string prefix)
        {
            if (prefix == null) throw new Exception("Prefix must not be null");
            if (!TryFindNodeIndex(prefix, out var currentNode)) yield break;

            var allKeys = _fwdCache[currentNode]?.Keys().ToArray();
            if (allKeys == null) yield break;

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
        [NotNull]public IEnumerable<string> GetPathsForEntry(TValue? value) {
            if (value == null) yield break;
            if (!_valueCache.ContainsKey(value) || _valueCache[value] == null) yield break;

            foreach (var index in _valueCache[value]!)
            {
                yield return TraceNodePath(index);
            }
        }

        /// <summary>
        /// Delete the value at a path if it exists. If the path doesn't exist or has no value, this command is ignored.
        /// </summary>
        public void Delete(string exactPath)
        {
            if (string.IsNullOrEmpty(exactPath)) throw new Exception("Path must not be null or empty");
            if (!TryFindNodeIndex(exactPath, out var currentNode)) return;
            if (_store[currentNode] == null) throw new Exception("Internal logic error in ReverseTrie.Delete()");
            var old = _store[currentNode]!.Data;
            _store[currentNode]!.Data = default;

            if (old != null && _valueCache.ContainsKey(old) && _valueCache[old] != null) {
                _valueCache[old]!.Remove(currentNode);
            }
        }

        /// <inheritdoc />
        public Stream Freeze()
        {
            // We only store the reverse list. The forward cache is always rebuilt.
            var ms = new MemoryStream();
            var dest = new BitwiseStreamWrapper(ms, 1);

            EncodeValue((uint)(_store.Count + 1), dest);

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

            // Write some zeros to pad the end of the stream
            EncodeValue(0, dest);// parent
            EncodeValue(0, dest);// value
            EncodeValue(0, dest);// data length
            dest.Flush();
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

        /// <inheritdoc />
        public void Defrost(Stream source)
        {
            var src = new BitwiseStreamWrapper(source, 64);

            // reset to starting condition
            _store.Clear();
            _fwdCache.Clear();
            RtNode.AddNewNode(RootValue, RootParent, _store);

            if (!TryDecodeValue(src, out var expectedLength)) {
                throw new Exception("Input stream is invalid");
            }
            if (expectedLength < 1) throw new Exception("Prefix length is invalid");
            expectedLength--;

            for (int i = 0; i < expectedLength; i++)
            {
                if (!TryDecodeValue(src, out var parent)) { break; }
                if (!TryDecodeValue(src, out var value)) throw new Exception("Invalid structure: Entry truncated at child");

                if (parent == 0 && value == 0) break; // hit an end-of-stream

                if (!TryDecodeValue(src, out var dataLength)) throw new Exception("Invalid structure: Entry truncated at data");

                if (parent > _store.Count) throw new Exception($"Invalid structure: found a parent forward of child (#{parent} of {_store.Count})");

                var newIdx = RtNode.AddNewNode((char)value, (int)parent, _store);
                if (newIdx <= parent) throw new Exception("Invalid structure: found a forward pointer");


                var map = _fwdCache[(int)parent] ?? throw new Exception("Internal storage error in ReverseTrie.Defrost()");
                map[(char)value] = newIdx;

                if (dataLength > 0) {
                    if (src.IsEmpty()) throw new Exception("Data declared in stream run-out");
                    var data = new TValue();
                    try
                    {
                        var subStream = new Substream(source, (int)dataLength);
                        if (subStream.AvailableData() < dataLength) throw new Exception($"Stream was not long enough for declared data (expected {dataLength}, got {subStream.AvailableData()})");
                        data.Defrost(subStream);
                        _store[newIdx]!.Data = data;
                        AddToValueCache(newIdx, data);
                    }
                    catch (Exception ex)
                    {
                        // What is going wrong here??
                        throw new Exception($"Failed to read data (declared length = {dataLength})", ex);
                    }
                }
            }
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


        private void AddToValueCache(int newIdx, [NotNull]TValue data)
        {
            if (!_valueCache.ContainsKey(data)) { _valueCache.Add(data, new HashSet<int>()); }
            _valueCache[data]?.Add(newIdx);
        }

        [NotNull, ItemNotNull]private IEnumerable<string> RecursiveSearch(int nodeIdx)
        {
            var node = _store[nodeIdx];
            if (node?.Data != null) {
                yield return TraceNodePath(nodeIdx);
            }

            var keys = _fwdCache[nodeIdx]?.Keys().ToArray();
            if (keys == null) throw new Exception();

            foreach (var nextChar in keys)
            {
                var child = _fwdCache[nodeIdx][nextChar];
                foreach (var str in RecursiveSearch(child)) {
                    yield return str;
                }
            }    
        }

        [NotNull]private string TraceNodePath(int nodeIdx)
        {
            // Trace from the node back to root, build a string
            var stack = new Stack<char>();
            while (nodeIdx > 0) {
                if (_store[nodeIdx] == null) throw new Exception("Internal storage error in ReverseTrie.TraceNodePath()");
                stack.Push(_store[nodeIdx]!.Value);
                nodeIdx = _store[nodeIdx]!.Parent;
            }
            var sb = new StringBuilder(stack.Count);
            while (stack.Count > 0) sb.Append(stack.Pop());
            return sb.ToString();
        }

        private bool TryFindNodeIndex([NotNull]string path, out int currentNode)
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
            var idx = RtNode.AddNewNode(c, currentNode, _store);

            var map = _fwdCache[currentNode];
            if (map == null) throw new Exception("Internal storage error in ReverseTrie.LinkNewNode()");
            map[c] = idx;
            return idx;
        }

        private int NextNode(int currentNode, char c)
        {
            var map = _fwdCache[currentNode];
            if (map == null) throw new Exception("Internal storage error in ReverseTrie.NextNode()");
            if (!map.Contains(c)) return -1;
            return map[c];
        }
        
        /// <summary>
        /// Read a value previously written with `EncodeValue`
        /// </summary>
        private static bool TryDecodeValue([NotNull]BitwiseStreamWrapper src, out uint value)
        {
            value = 0;
            var ok = src.TryReadBit_RO(out var b);
            if (!ok) return false;

            if (b == 0) { // one byte (7 bit data)
                for (int i = 0; i < 7; i++) {
                    value |= (uint)(src.ReadBit() << i);
                }
                return true;
            }
            
            ok = src.TryReadBit_RO(out b);
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
        private static void EncodeValue(uint value, [NotNull]BitwiseStreamWrapper dest)
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