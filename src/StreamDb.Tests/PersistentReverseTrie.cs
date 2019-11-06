using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using StreamDb.Internal.Support;

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

            var subject = new ReverseTrie();

            subject.Add("Hello world");
            subject.Add("Hello Dave");

            Console.WriteLine($"\r\nResult:\r\n{subject.DiagnosticDescription()}");
        }
    }

    public class ReverseTrie
    {
        public class RTNode:PartiallyOrdered {
            private static readonly object _lock = new object();
            public readonly char Value;
            public readonly int Parent;

            /// <summary>This is set during storage to help lookups </summary>
            public int SelfIndex;

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

        /// <summary>This is the core list used for storage, and produces indexes</summary>
        private readonly List<RTNode> _store;

        /// <summary> (Node->SelfIndex); This is the 'forward pointing cache' we use during construction.</summary>
        private readonly Dictionary<RTNode, int> _fwdCache;

        public ReverseTrie()
        {
            _store = new List<RTNode>();
            _fwdCache = new Dictionary<RTNode, int>();

            RTNode.AddNewNode(RootValue, RootParent, _store);
        }

        /// <summary>
        /// Add a new path to the trie
        /// TODO: link data at the end!
        /// </summary>
        /// <param name="path"></param>
        public void Add(string path)
        {
            // Plan: use the _fwdCache to walk, when there is no more path, start adding.
            var q = new Queue<char>(path);

            var currentNode = 0; // root is always at zero

            while (q.Count > 0)
            {
                var c = q.Dequeue();

                // Find link from current to next (will continually fail when we're writing. This could be optimised)
                var next = NextNode(currentNode, c);
                if (next > 0) {
                    currentNode = next;
                    Console.Write($".{c}");
                    continue;
                }

                // Not found. Add a new node linked back.
                currentNode = LinkNewNode(currentNode, c);

                Console.Write($"!{c}");
            }

            Console.WriteLine();
        }

        /// <summary>
        /// Provide a human readable string of the storage list. Does not include the forward cache
        /// </summary>
        public string DiagnosticDescription()
        {
            var sb = new StringBuilder();

            foreach (var node in _store)
            {
                sb.Append($"({node.Value}[{node.SelfIndex}]->{node.Parent})");
            }

            return sb.ToString();
        }

        private int LinkNewNode(int currentNode, char c)
        {
            var idx = RTNode.AddNewNode(c, currentNode, _store);
            _fwdCache.Add(new RTNode(c, currentNode), idx); // should be unique
            return idx;
        }

        private int NextNode(int currentNode, char c)
        {
            var key = new RTNode(c, currentNode);
            if (!_fwdCache.ContainsKey(key)) return -1;
            return _fwdCache[key];
        }
    }
}