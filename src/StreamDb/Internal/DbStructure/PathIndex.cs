using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using JetBrains.Annotations;
using StreamDb.Internal.Support;

namespace StreamDb.Internal.DbStructure
{
    /// <summary>
    /// Provides Path->ID indexing
    /// </summary>
    /// <remarks>
    /// TinyDB stores files by GUID, and the file name is stored inside the entry.
    /// The result of path index is stored as a special document in the database, and used
    /// to look up files by path.
    /// </remarks>
    public class PathIndex<T>: IByteSerialisable where T : IByteSerialisable, new()
    {
        // Flag values
        const byte HAS_MATCH = 1 << 0;
        const byte HAS_LEFT = 1 << 1;
        const byte HAS_RIGHT = 1 << 2;
        const byte HAS_DATA = 1 << 3;

        const long INDEX_MARKER = 0xFACEFEED; // 32 bits of zero, then the magic number
        const long DATA_MARKER = 0xBACCFACE;
        const long END_MARKER = 0xDEADBEEF;

        const int EMPTY_OFFSET = -1; // any pointer that is not set

        private class Node
        {
            public char Ch; // the path character at this step
            public int Left, Match, Right; // Indexes into the node array
            public int DataIdx; // Index into entries array. If -1, this is not a path endpoint
            public Node() { Left = Match = Right = DataIdx = EMPTY_OFFSET; }
        }

        [NotNull, ItemNotNull]private readonly List<Node> _nodes;
        [NotNull, ItemNotNull]private readonly List<T> _entries;

        public PathIndex() { _nodes = new List<Node>(); _entries = new List<T>(); }

        /// <summary>
        /// Insert a path/value pair into the index.
        /// If a value already existed for the path, it will be replaced and the old value returned
        /// </summary>
        public T Add(string path, T value)
        {
            if (string.IsNullOrEmpty(path)) return default;

            var nodeIdx = EnsurePath(path);

            var node = _nodes[nodeIdx];
            var oldValue = GetValue(node.DataIdx);
            SetValue(nodeIdx, value);
            return oldValue;
        }

        /// <summary>
        /// Get a value by exact path.
        /// If the path has no value, NULL will be returned
        /// </summary>
        public T Get(string exactPath)
        {
            if (string.IsNullOrEmpty(exactPath)) return default;

            var nodeIdx = WalkPath(exactPath);
            if (nodeIdx < 0 || nodeIdx >= _nodes.Count) return default;
            var node = _nodes[nodeIdx];
            return GetValue(node.DataIdx);
        }

        /// <summary>
        /// Delete the value for a key, by exact path.
        /// If the path has no value, nothing happens
        /// </summary>
        public void Delete(string exactPath)
        {
            if (string.IsNullOrEmpty(exactPath)) return;

            var nodeIdx = WalkPath(exactPath);
            if (nodeIdx < 0 || nodeIdx >= _nodes.Count) return;
            var node = _nodes[nodeIdx];
            SetValue(node.DataIdx, default);
            node.DataIdx = EMPTY_OFFSET;
        }
        
        /// <summary>
        /// Return all known paths that start with the given prefix
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        [NotNull, ItemNotNull]public IEnumerable<string> Search(string prefix)
        {
            if (string.IsNullOrEmpty(prefix)) return new string[0];

            // get a node for the prefix
            var nodeIdx = WalkPath(prefix);
            var result = new List<string>();

            // now recurse down the tree and list out all possibilities
            var start = prefix.Substring(0, prefix.Length - 1);
            RecurseIntoList(start, nodeIdx, result);

            return result;
        }

        private void RecurseIntoList(string prefix, int nodeIdx, [NotNull]List<string> result)
        {
            if (nodeIdx < 0 || nodeIdx >= _nodes.Count) return;

            var node = _nodes[nodeIdx];

            if (node.DataIdx >= 0) result.Add(prefix + node.Ch);

            if (node.Match >= 0) {
                RecurseIntoList(prefix + node.Ch, node.Match, result);
            }

            if (node.Left >= 0) {
                RecurseIntoList(prefix, node.Left, result);
            }

            if (node.Right >= 0) {
                RecurseIntoList(prefix, node.Right, result);
            }
        }

        private int WalkPath([NotNull]string path)
        {
            int curr = 0, next = 0, cpos = 0;
            while (cpos < path.Length)
            {
                if (next < 0) return EMPTY_OFFSET;
                curr = next;

                var ch = path[cpos];
                next = ReadStep(curr, ch, ref cpos);
            }
            return curr;
        }

        private int ReadStep(int idx, char ch, ref int matchIncr)
        {
            if (_nodes.Count < 1) { return EMPTY_OFFSET; } // empty

            var inspect = _nodes[idx];

            if (inspect.Ch == 0) { return EMPTY_OFFSET; } // empty match. No key here.  
            if (inspect.Ch == ch) { matchIncr++; return inspect.Match; }

            // can't follow the straight line. Need to branch
            return ch < inspect.Ch ? inspect.Left : inspect.Right;
        }

        private int EnsurePath([NotNull]string path)
        {
            int curr = 0, next = 0, cpos = 0;
            while (cpos < path.Length)
            {
                curr = next;

                var ch = path[cpos];
                next = BuildStep(curr, ch, ref cpos);
            }
            return curr;
        }

        private int BuildStep(int idx, char ch, ref int matchIncr)
        {
            if (_nodes.Count < 1) { return NewIndexNode(ch); } // empty

            var inspect = _nodes[idx];

            if (inspect.Ch == 0) { // empty match. Fill it in
                inspect.Ch = ch;
                if (inspect.Match > EMPTY_OFFSET) throw new Exception("invalid match structure");
                inspect.Match = NewEmptyIndex(); // next empty match
                return idx;
            }

            if (inspect.Ch == ch)
            {
                matchIncr++;
                if (inspect.Match < 0) { inspect.Match = NewEmptyIndex(); }
                return inspect.Match;
            }

            // can't follow the straight line. Need to branch

            if (ch < inspect.Ch) { // switch left
                if (inspect.Left >= 0) return inspect.Left;

                // add new node for this value, increment match
                inspect.Left = NewIndexNode(ch);
                _nodes[inspect.Left].Match = NewEmptyIndex();
                return inspect.Left;
            }

            // switch right
            if (inspect.Right >= 0) return inspect.Right;
            // add new node for this value, increment match
            inspect.Right = NewIndexNode(ch);
            _nodes[inspect.Right].Match = NewEmptyIndex();
            return inspect.Right;
        }

        private int NewIndexNode(char ch)
        {
            var node = new Node {Ch = ch};
            var idx = _nodes.Count;
            _nodes.Add(node);
            return idx;
        }

        private int NewEmptyIndex()
        {
            var node = new Node {Ch = (char) 0};
            var idx = _nodes.Count;
            _nodes.Add(node);
            return idx;
        }

        private void SetValue(int nodeIdx, T value)
        {
            if (nodeIdx < 0) throw new Exception("node index makes no sense");
            if (nodeIdx >= _nodes.Count) throw new Exception("node index makes no sense");

            var newIdx = _entries.Count;
            _entries.Add(value);

            _nodes[nodeIdx].DataIdx = newIdx;
        }

        private T GetValue(int nodeDataIdx)
        {
            if (nodeDataIdx < 0) return default;
            if (nodeDataIdx >= _entries.Count) return default;
            return _entries[nodeDataIdx];
        }

        public string DiagnosticString()
        {
            var sb = new StringBuilder();

            sb.AppendLine("INDEX: ");
            int i = 0;
            foreach (var node in _nodes)
            {
                sb.Append("    ");
                sb.Append(i);
                sb.Append("['");
                sb.Append(node.Ch);
                sb.Append("', D=");
                sb.Append(node.DataIdx);
                sb.Append(", L=");
                sb.Append(node.Left);
                sb.Append(", M=");
                sb.Append(node.Match);
                sb.Append(", R=");
                sb.Append(node.Right);
                sb.AppendLine("];");
                i++;
            }

            sb.AppendLine("DATA: ");
            i = 0;
            foreach (var entry in _entries)
            {
                sb.Append("    ");
                sb.Append(i);
                sb.Append("[");
                sb.Append(entry);
                sb.AppendLine("];");
                i++;
            }

            return sb.ToString();
        }

        /// <summary>
        /// Write a serialised form to the stream at its current position
        /// </summary>
        public void WriteTo(Stream stream)
        {
            if (stream == null) return;
            using (var w = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                // TODO: I would like an append-only format for this,
                //        so we don't have to re-write the whole structure
                //        for every change, wasting time and space.
                w.Write(INDEX_MARKER);
                w.Write(_nodes.Count);
                foreach (var node in _nodes) { WriteIndexNode(node, w); }

                w.Write(DATA_MARKER);
                w.Write(_entries.Count);
                foreach (var entry in _entries) { WriteDataEntry(entry, w); }
                w.Write(END_MARKER);
            }
        }

        /// <summary>
        /// Read a stream (previously written by `WriteTo`) from its current position
        /// into a new index. Will throw an exception if the data is not consistent and complete.
        /// </summary>
        [NotNull] public static PathIndex<T> ReadFrom(Stream stream)
        {
            if (stream == null) throw new ArgumentNullException(nameof(stream));
            var result = new PathIndex<T>();
            OverwriteFromStream(stream, result);
            return result;
        }

        private static void OverwriteFromStream([NotNull]Stream stream, [NotNull]PathIndex<T> result)
        {
            using (var r = new BinaryReader(stream, Encoding.UTF8, true))
            {
                if (r.ReadInt64() != INDEX_MARKER) throw new Exception("Input stream missing index marker");
                var nodeCount = r.ReadInt32();
                if (nodeCount < 0) throw new Exception("Input stream node count invalid");

                for (int i = 0; i < nodeCount; i++)
                {
                    result._nodes.Add(ReadIndexNode(r));
                }

                if (r.ReadInt64() != DATA_MARKER) throw new Exception("Input stream missing data marker");
                var entryCount = r.ReadInt32();
                if (entryCount < 0) throw new Exception("Input stream node count invalid");

                for (int i = 0; i < entryCount; i++)
                {
                    result._entries.Add(ReadDataEntry(r));
                }

                if (r.ReadInt64() != END_MARKER) throw new Exception("Input stream missing end marker");
            }
        }

        private static T ReadDataEntry([NotNull]BinaryReader r)
        {
            var length = r.ReadInt32();
            if (length < 0) return default;
            if (length == 0) return default;

            var bytes = r.ReadBytes(length);

            var value = new T();
            value.FromBytes(bytes);
            return value;
        }

        private void WriteDataEntry(T data, [NotNull]BinaryWriter w)
        {
            if (data == null) { w.Write(EMPTY_OFFSET); return; }
            var bytes = data.ToBytes();
            w.Write(bytes.Length);
            w.Write(bytes);
        }

        private static Node ReadIndexNode([NotNull]BinaryReader r)
        {
            var node = new Node {Ch = r.ReadChar()};


            var flags = r.ReadByte();
            if ((flags & HAS_MATCH) > 0) node.Match = r.ReadInt32();
            if ((flags & HAS_LEFT) > 0) node.Left = r.ReadInt32();
            if ((flags & HAS_RIGHT) > 0) node.Right = r.ReadInt32();
            if ((flags & HAS_DATA) > 0) node.DataIdx = r.ReadInt32();

            return node;
        }

        private static void WriteIndexNode([NotNull]Node node, [NotNull]BinaryWriter w)
        {
            byte flags = 0;
            if (node.Match >= 0) flags |= HAS_MATCH;
            if (node.Left >= 0) flags |= HAS_LEFT;
            if (node.Right >= 0) flags |= HAS_RIGHT;
            if (node.DataIdx >= 0) flags |= HAS_DATA;

            w.Write(node.Ch);
            w.Write(flags);

            if (node.Match >= 0) w.Write(node.Match);
            if (node.Left >= 0) w.Write(node.Left);
            if (node.Right >= 0) w.Write(node.Right);
            if (node.DataIdx >= 0) w.Write(node.DataIdx);
        }

        /// <inheritdoc />
        public byte[] ToBytes()
        {
            using (var ms = new MemoryStream()) {
                WriteTo(ms);
                ms.Seek(0, SeekOrigin.Begin);
                return ms.ToArray() ?? throw new Exception();
            }
        }

        /// <inheritdoc />
        public void FromBytes(byte[] source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            using (var ms = new MemoryStream(source)) {
                ms.Seek(0, SeekOrigin.Begin);
                OverwriteFromStream(ms, this);
            }
        }
    }
}