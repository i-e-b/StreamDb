using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    public class PathIndex<T>: IStreamSerialisable where T : PartiallyOrdered, IStreamSerialisable, new()
    {
        // Serialisation tags:
        const byte START_MARKER = 0xFF;
        const byte INDEX_MARKER = 0xF0;
        const byte DATA_MARKER  = 0x0F;
        const byte END_MARKER   = 0x55;
        const byte STREAM_ENDED = 0xAA; // not written, but signals that we got to the end of the data being read.

        const int EMPTY_OFFSET = -1; // any pointer that is not set

        /// <summary> Trie node, in memory representation (forward links) </summary>
        /// <remarks>The serialised form used in storage is very different, and uses back-pointing links</remarks>
        private class Node
        {
            public char Ch; // the path character at this step
            public int Left, Match, Right; // Indexes into the node array
            public int DataIdx; // Index into entries array. If -1, this is not a path endpoint
            public BackLink Backlink; // reverse link, used to recover a path from an arbitrary node
            public Node() { Left = Match = Right = DataIdx = EMPTY_OFFSET; }
        }
        
        enum BackLinkType:byte { None=0, Left=0b11, Right = 0b1100, Match = 0b110000 }
        struct BackLink { 
            /// <summary> Absolution position of the parent entry that contained the original forward link </summary>
            public int Position;
            /// <summary> Which branch was this child on? </summary>
            public BackLinkType Type;
        }

        /// <summary> Data of type `T`, plus some book-keeping data </summary>
        private class Entry {
            /// <summary> The data of this entry. This is serialised. </summary>
            public T Data;
            /// <summary> The index in the `nodes` array that links to this entry. Used for searchin. This is serialised. </summary>
            public int NodeIndex;
            /// <summary> The length of the `nodes` array when this entry was added </summary>
            /// <remarks> This is not serialised. We use it for sorting, and is restored implicitly when deserialising. </remarks>
            public int NodePosition;
            /// <summary> Original position </summary>
            /// <remarks> This is not serialised. We use it for sorting, and is restored implicitly when deserialising. </remarks>
            public int EntryOrder;
        }
        
        /// <summary>
        /// Provides a stable sort over entry metadata
        /// </summary>
        class EntrySerialOrder : IComparer<Entry> {
            /// <inheritdoc />
            public int Compare(Entry x, Entry y)
            {
                if (x == null || y == null) return 0; // should never happen
                if (x.NodePosition == y.NodePosition) return x.EntryOrder.CompareTo(y.EntryOrder);
                return x.NodePosition.CompareTo(y.NodePosition);
            }
        }

        [NotNull, ItemNotNull]private readonly List<Node> _nodes;
        [NotNull, ItemNotNull]private readonly List<Entry> _entries;

        public PathIndex() { _nodes = new List<Node>(); _entries = new List<Entry>(); }

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
            RecurseIntoList(nodeIdx, result);

            return result;
        }


        /// <summary>
        /// Scan the index, return all paths that match the given value.
        /// Comparison is done by the the comparison method of the value's type.
        /// If no matches are found, the enumeration will be empty.
        /// </summary>
        /// <param name="value">A stored value to search</param>
        /// <returns>All matching paths</returns>
        [NotNull, ItemNotNull]
        public IEnumerable<string> GetPathsForEntry(T value)
        {
            if (value == null) yield break;

            foreach (var entry in _entries)
            {
                if (entry.Data != value) continue;
                if (entry.NodeIndex < 0) continue;

                // Now reconstruct matching path
                yield return PathToNode(entry.NodeIndex);
            }
        }

        /// <summary>
        /// recover a path from a node
        /// </summary>
        [NotNull]private string PathToNode(int nodeIdx)
        {
            // starting from the node, we work backward then build a string forward
            // this relies on the backlinks.

            var node = _nodes[nodeIdx];
            var stack = new Stack<char>();

            stack.Push(node.Ch);

            while (node.Backlink.Type != BackLinkType.None) {
                var lastIdx = node.Backlink.Position;
                var type = node.Backlink.Type;
                node = _nodes[node.Backlink.Position];
                if (node.Backlink.Position < 0) break;

                if (type == BackLinkType.Match)
                {
                    stack.Push(node.Ch);
                }

                if (lastIdx == 0) break;
            }

            // flip the string over
            var sb = new StringBuilder();
            while (stack.Count > 0) sb.Append(stack.Pop());
            return sb.ToString();
        }

        private void RecurseIntoList(int nodeIdx, [NotNull]List<string> result)
        {
            if (nodeIdx < 0 || nodeIdx >= _nodes.Count) return;

            var node = _nodes[nodeIdx];

            //if (node.DataIdx >= 0) result.Add(prefix + node.Ch);
            if (node.DataIdx >= 0) result.Add(PathToNode(nodeIdx));

            if (node.Match >= 0) {
                RecurseIntoList(node.Match, result);
            }

            if (node.Left >= 0) {
                RecurseIntoList(node.Left, result);
            }

            if (node.Right >= 0) {
                RecurseIntoList(node.Right, result);
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
                var isLast = cpos == path.Length - 1;
                next = BuildStep(curr, ch, isLast, ref cpos);
            }
            return curr;
        }

        private int BuildStep(int idx, char ch, bool isLast, ref int matchIncr)
        {
            if (_nodes.Count < 1) { return NewIndexNode(ch, 0, BackLinkType.None); } // empty

            var inspect = _nodes[idx];

            if (inspect.Ch == 0) { // empty match. Fill it in
                inspect.Ch = ch;
                inspect.Backlink.Type = BackLinkType.Match;
                if (inspect.Match > EMPTY_OFFSET) throw new Exception("invalid match structure");
                if (!isLast) inspect.Match = NewEmptyIndex(idx); // next empty match ready for the rest of the string (only if there is more string)
                return idx;
            }

            if (inspect.Ch == ch)
            {
                matchIncr++;
                if (inspect.Match < 0 && !isLast) { inspect.Match = NewEmptyIndex(idx); }
                return inspect.Match;
            }

            // can't follow the straight line. Need to branch

            if (ch < inspect.Ch) { // switch left
                if (inspect.Left >= 0) return inspect.Left;

                // add new node for this value, increment match
                inspect.Left = NewIndexNode(ch, idx, BackLinkType.Left);
                if (!isLast) _nodes[inspect.Left].Match = NewEmptyIndex(inspect.Left);
                return inspect.Left;
            }

            // switch right
            if (inspect.Right >= 0) return inspect.Right;
            // add new node for this value, increment match
            inspect.Right = NewIndexNode(ch, idx, BackLinkType.Right);
            if (!isLast) _nodes[inspect.Right].Match = NewEmptyIndex(inspect.Right);
            return inspect.Right;
        }

        private int NewIndexNode(char ch, int parentIdx, BackLinkType branchType)
        {
            var node = new Node {
                Ch = ch,
                Backlink = new BackLink {
                    Position = parentIdx,
                    Type = branchType
                }
            };
            var idx = _nodes.Count;
            _nodes.Add(node);
            return idx;
        }

        private int NewEmptyIndex(int parentIdx)
        {
            var node = new Node
            {
                Ch = (char)0,
                Backlink = new BackLink
                {
                    Position = parentIdx,
                    Type = BackLinkType.Match
                }
            };
            var idx = _nodes.Count;
            _nodes.Add(node);
            return idx;
        }

        private void SetValue(int nodeIdx, T value)
        {
            if (nodeIdx < 0) throw new Exception("node index makes no sense");
            if (nodeIdx >= _nodes.Count) throw new Exception("node index makes no sense");

            var newIdx = _entries.Count;
            var entry = new Entry{
                Data = value,
                EntryOrder = newIdx,
                NodePosition = _nodes.Count,
                NodeIndex = nodeIdx
            };
            _entries.Add(entry);

            _nodes[nodeIdx].DataIdx = newIdx;
        }

        private T GetValue(int nodeDataIdx)
        {
            if (nodeDataIdx < 0) return default;
            if (nodeDataIdx >= _entries.Count) return default;
            return _entries[nodeDataIdx].Data;
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
                sb.Append(", ");
                sb.Append(node.Backlink.Position);
                sb.Append("<- ");
                sb.Append(node.Backlink.Type);
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
                sb.Append(entry.Data);
                sb.AppendLine("];");
                i++;
            }

            return sb.ToString();
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

        /// <summary>
        /// Write a serialised form to the stream at its current position
        /// </summary>
        public void WriteTo(Stream stream)
        {
            if (stream == null) return;
            
            // Plan: keep the nodes and entries together in the serialised form
            //
            // Have a temp array 1:1 with the entries array. Each slot holds one of [ no-link | left | right | middle ] and an index.
            // When we serialise, we don't write the forward links, but fill in the entry for the target.
            // If the entry we are serialising has something other than 'no-link' in its slot, we write that in the output at that point.
            // When deserialising, when we come across a back link, we fill in the reverse target to make it a forwards link again.
            //
            // We need to output data entries in a stable location, after any data we have already written.
            // To do this, we keep track of how long the node array was when the node data was set, and where it was in the data list
            // We then sort by those, and output the data in 'node chronological' order.

            var linkMeta = new BackLink[_nodes.Count];
            var sortedEntries = _entries.OrderBy(k=>k, new EntrySerialOrder()).ToArray();
            var dataIndex = 0; // this is our seek through sortedEntries

            using (var w = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                w.Write(START_MARKER);

                for (var i = 0; i < _nodes.Count; i++)
                {
                    while (dataIndex < sortedEntries.Length && sortedEntries[dataIndex] != null
                                                            && sortedEntries[dataIndex].NodePosition <= i) {
                        
                        w.Write(DATA_MARKER);
                        WriteDataEntry(sortedEntries[dataIndex], w);
                        dataIndex++;
                    }


                    var node = _nodes[i];

                    // mark up the backlink meta data
                    if (node.Left >= 0) {
                        if (node.Left <= i) throw new Exception("inverse left link");
                        linkMeta[node.Left] = new BackLink {Type = BackLinkType.Left, Position = i};
                    }
                    if (node.Right >= 0) {
                        if (node.Right <= i) throw new Exception("inverse right link");
                        linkMeta[node.Right] = new BackLink {Type = BackLinkType.Right, Position = i};
                    }
                    if (node.Match >= 0) {
                        if (node.Match <= i) throw new Exception("inverse match link");
                        linkMeta[node.Match] = new BackLink {Type = BackLinkType.Match, Position = i};
                    }

                    w.Write(INDEX_MARKER);

                    w.Write(node.Ch);
                    w.Write(node.DataIdx); // will be -1 if no data. We should be recovering the same entry indexes.
                    w.Write((byte)linkMeta[i].Type);
                    w.Write(linkMeta[i].Position); // every node except the root should have a backlink

                }
                // write out any data nodes that are left
                while (dataIndex < sortedEntries.Length && sortedEntries[dataIndex] != null)
                {
                    w.Write(DATA_MARKER);
                    WriteDataEntry(sortedEntries[dataIndex], w);
                    dataIndex++;
                }

                w.Write(END_MARKER);
            }
        }


        private static void OverwriteFromStream([NotNull]Stream stream, [NotNull]PathIndex<T> result)
        {
            using (var r = new BinaryReader(stream, Encoding.UTF8, true))
            {
                // See `WriteTo()` for a description of this.
                // The tree is stored with links backwards, to make it an append-only structure
                // When reading, with flip them back to forwards links for querying

                var marker = stream.ReadByte();
                if (marker != START_MARKER) throw new Exception($"Input stream missing start marker. Expected {START_MARKER:X}, but got {marker:X}");
                
                // we could get index nodes or data entries in any order
                while (true)
                {
                    var tag = TryReadTag(stream);
                    switch (tag) {
                        case END_MARKER:
                            // Thought: maybe allow multiple of these, so we can recover bad concatenation?
                            //          In which case, an end-marker is more like a 'commit'.
                            break;
                        case START_MARKER:
                            // This should only happen after an 'END_MARKER'
                            break;
                        case STREAM_ENDED:
                            // if we are doing 'commits', this would abandon any data not yet comitted.
                            return;
                        case INDEX_MARKER:
                            {
                                var node = new Node {
                                    Ch = r.ReadChar(),
                                    DataIdx = r.ReadInt32(),
                                    Backlink = new BackLink{
                                        Type = (BackLinkType)r.ReadByte(),
                                        Position = r.ReadInt32()
                                    }
                                };
                                
                                result._nodes.Add(node);
                                StitchLink(result, node.Backlink, result._nodes.Count - 1);
                            }
                            break;
                        case DATA_MARKER:
                            {
                                var data = ReadDataEntry(r, out var nodeIdx);
                                var entry = new Entry
                                {
                                    Data = data,
                                    NodeIndex = nodeIdx,
                                    NodePosition = result._nodes.Count,
                                    EntryOrder = result._entries.Count,
                                    // TODO: how do we link back to our node index? Rescan after?
                                };
                                result._entries.Add(entry);
                            }
                            break;
                        default:
                            throw new Exception($"PathIndex.OverwriteFromStream: Invalid serialisation structure ({tag:X}) at position {stream.Position} of {stream.Length}");
                    }
                }
            }
        }

        private static byte TryReadTag([NotNull]Stream r)
        {
            if (r.Position == r.Length) return STREAM_ENDED;

            try {
                var v = r.ReadByte();
                if (v < 0) return STREAM_ENDED;
                return (byte)v;
            } catch {
                return STREAM_ENDED;
            }
        }

        private static void StitchLink([NotNull]PathIndex<T> result, BackLink backLink, int targetOffset)
        {
            if (backLink.Type == BackLinkType.None) return;

            var blp = backLink.Position;
            if (blp < 0) throw new Exception("PathIndex.StitchLink: back link was negative");
            if (blp >= result._nodes.Count) throw new Exception($"PathIndex.StitchLink: back link was a forward link. P={blp}; O={targetOffset}; L={result._nodes.Count}.");
            var node =  result._nodes[blp];
            if (node == null) throw new Exception("Deserialisation desynchronised");
            switch (backLink.Type) {

                case BackLinkType.Left:
                    node.Left = targetOffset;
                    return;
                    
                case BackLinkType.Right:
                    node.Right = targetOffset;
                    return;

                case BackLinkType.Match:
                    node.Match = targetOffset;
                    return;

                case BackLinkType.None:
                    return;

                default:
                    throw new Exception("Non exhaustive switch in PathIndex.StitchLink");
            }
        }

        private static T ReadDataEntry([NotNull]BinaryReader r, out int nodeIdx)
        {
            var length = r.ReadInt32();
            nodeIdx = (length < 0) ? -1 : r.ReadInt32();
            if (length <= 0) return default;
            
            var value = new T();
            value.Defrost(new Substream(r.BaseStream, length));
            return value;
        }

        private void WriteDataEntry(Entry entry, [NotNull]BinaryWriter w)
        {
            if (entry?.Data == null) { w.Write(EMPTY_OFFSET); return; }

            var bytes = entry.Data.Freeze();
            w.Write((int)bytes.Length);
            w.Write(entry.NodeIndex);
            bytes.CopyTo(w.BaseStream);
        }

        /// <inheritdoc />
        public Stream Freeze()
        {
            var ms = new MemoryStream();
            WriteTo(ms);
            ms.Seek(0, SeekOrigin.Begin);
            return ms;
        }

        /// <inheritdoc />
        public void Defrost(Stream source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            OverwriteFromStream(source, this);
        }
    }
}