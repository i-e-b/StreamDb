﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace StreamDb.Tests
{
    [TestFixture]
    public class UkkonenSuffixTreeTests
    {
        // https://en.wikipedia.org/wiki/Ukkonen%27s_algorithm
        // https://stackoverflow.com/questions/9452701/ukkonens-suffix-tree-algorithm-in-plain-english/9513423#9513423
        // https://marknelson.us/posts/1996/08/01/suffix-trees.html
        // http://programmerspatch.blogspot.com/2013/02/ukkonens-suffix-tree-algorithm.html
        // http://brenden.github.io/ukkonen-animation/

        //                                           1         2         3         4         5         6         7 
        //                                 012345678901234567890123456789012345678901234567890123456789012345678901
        private const string SampleText = "how much wood would a wood chuck chuck if a wood chuck could chuck wood";

        [Test]
        public void construction_from_text ()
        {
            var subject = new SuffixTree();
            subject.Extend(SampleText);

            var desc = subject.TreeDescription();
            Console.WriteLine(desc);
        }

        [Test]
        public void query_for_existence () {
            var subject = new SuffixTree();
            subject.Extend(SampleText);

            Assert.That(subject.Contains("much wood"), Is.True);
            Assert.That(subject.Contains("uck ch"), Is.True);
            Assert.That(subject.Contains("k if a"), Is.True);
            Assert.That(subject.Contains("wooden"), Is.False);
            Assert.That(subject.Contains("wwood"), Is.False);
        }

        [Test]
        public void query_for_all_positions_simple() {
            var subject = new SuffixTree();
            subject.Extend("abcabxabcd$");
            subject.Terminate(); // required to catch the last one

            //var desc = subject.TreeDescription();
            //Console.WriteLine(desc);
            
            var positions = subject.FindAll("abc").ToList().OrderBy(n=>n);
            var result = string.Join(",", positions);
            Assert.That(result, Is.EqualTo("0,6"));

            
            positions = subject.FindAll("b").ToList().OrderBy(n=>n);
            result = string.Join(",", positions);
            Assert.That(result, Is.EqualTo("1,4,7"));
        }

        [Test]
        public void query_for_all_positions () {
            var subject = new SuffixTree();
            subject.Extend(SampleText);
            subject.Terminate(); // required to catch the last one

            //var desc = subject.TreeDescription();
            //Console.WriteLine(desc);
            
            var positions = subject.FindAll("wood").ToList().OrderBy(n=>n);
            var result = string.Join(",", positions);
            Assert.That(result, Is.EqualTo("9,22,44,67"), "positions of 'wood'");

            
            positions = subject.FindAll("chuck").ToList().OrderBy(n=>n);
            result = string.Join(",", positions);
            Assert.That(result, Is.EqualTo("27,33,49,61"), "positions of 'chuck'");
            
        }
    }

    public class SuffixNode {
        public int Start;
        public int End;
        public int SuffixLink;
        public Map<char,int> Next;

        public SuffixNode()
        {
            // This could be <int,int>, and use -1 as the special termination marker?
            Next = new Map<char, int>();
        }

        public int EdgeLength(int position) => Math.Min(End, position + 1) - Start;
        public bool IsLeaf() => Next.IsEmpty();
    }

    public class Map<TIdx, TVal> where TIdx:struct
    {
        private readonly Dictionary<TIdx, TVal> _data;

        public TVal this[TIdx index]
        {
            get
            {
                if (_data.ContainsKey(index)) return _data[index];
                return default;
            }
            set
            {
                if (_data.ContainsKey(index)) _data[index] = value;
                else _data.Add(index, value);
            }
        }

        public IEnumerable<TIdx> Keys() => _data.Keys;
        public bool Contains(TIdx idx) => _data.ContainsKey(idx);
        public bool IsEmpty() => _data.Count < 1;
        public IEnumerable<KeyValuePair<TIdx, TVal>> All() => _data.Select(a=>a);

        public Map() { _data = new Dictionary<TIdx, TVal>(); }

    }

    public class SuffixTree
    {
        public const int Infinity = 1 << 29; // Special value marking 'until end', sometimes '#' in papers.

        readonly int _root;
        int _pos, _needSLink, _remainder, _activeNode, _activeEdge, _activeLength;

        readonly List<SuffixNode> _tree;
        readonly List<char> _text;

        public SuffixTree()
        {
            _tree = new List<SuffixNode>();
            _text = new List<char>();

            _needSLink = 0;
            _remainder = 0;
            _activeNode = 0;
            _activeEdge = 0;
            _activeLength = 0;

            _pos = -1;
            _root = AddNode(-1, -1);
            _activeNode = _root;
        }
        

        /// <summary>
        /// Debug description of the tree in its current state
        /// </summary>
        /// <returns></returns>
        public string TreeDescription()
        {
            var sb = new StringBuilder();

            DescribeNodeRecursive(sb, _root, 0);

            return sb.ToString();
        }

        private void DescribeNodeRecursive(StringBuilder sb, int idx, int depth)
        {
            var node = _tree[idx];
            sb.Append(NodeText(node));
            var end = Math.Min(_text.Count-1, node.End);
            sb.Append($" [{idx}: ^{node.Start} {end}$");
            if (node.SuffixLink > 0) {
                sb.Append($" SL={node.SuffixLink}");
            }
            sb.Append("] (");

            var keys = node.Next.Keys().ToArray();
            if (keys.Length > 0)
            {
                foreach (var nextChar in keys)
                {
                    sb.Append("\r\n");
                    sb.Append(' ', depth);

                    DescribeNodeRecursive(sb, node.Next[nextChar], depth + 1);
                }
                sb.Append("\r\n");
                sb.Append(' ', depth);
            }
            sb.Append(") ");

        }

        private int EdgeLength(SuffixNode node) {
            var stop = Math.Min(node.End, _text.Count);
            if (node.Start == stop) return 1;
            return stop - node.Start;
        }

        private string NodeText(SuffixNode node)
        {
            var stop = Math.Min(node.End, _text.Count);
            if (node.Start == stop) return "";

            var sb = new StringBuilder();
            for (int i = node.Start; i < stop; i++)
            {
                sb.Append(_text[i]);
            }
            return sb.ToString();
        }


        /// <summary>
        /// Returns true if the pattern exists at least once in the source text.
        /// Search is case sensitive
        /// </summary>
        public bool Contains(string pattern)
        {
            var index = FindNodeIndex(pattern, out _);
            return index >= 0;
        }

        
        /// <summary>
        /// List all positions in the source text where the pattern is found.
        /// Returns empty if the pattern is not found.
        /// If an empty pattern is given, an empty set is returned (technically, it should give every position in the string). 
        /// </summary>
        public IEnumerable<int> FindAll(string pattern)
        {
            // Find the pattern our tree (as close to the root as possible)
            // All suffix positions below this point are occurances of the pattern

            var start = FindNodeIndex(pattern, out var remains);
            if (start <= 0) yield break; // not found, or empty pattern.

            var startNode = _tree[start];
            var prime = pattern.Length - (EdgeLength(startNode) - remains);

            // Walk the tree, yield leaf node positions
            foreach (var idx in FindLeafsRec(start, prime)) yield return idx;
        }



        private IEnumerable<int> FindLeafsRec(int nodeIdx, int pathLength)
        {
            var node = _tree[nodeIdx];
            var len = EdgeLength(node);

            if (node.IsLeaf())
            {
                var loc = _text.Count - (pathLength + len);
                yield return loc;
            }

            foreach (char key in node.Next.Keys())
            {
                var nextSet = FindLeafsRec(node.Next[key], pathLength + len);
                foreach (var position in nextSet)
                {
                    yield return position;
                }
            }

        }

        /// <summary>
        /// Extend the tree with more text
        /// </summary>
        public void Extend(string text)
        {
            foreach (char c in text)
            {
                ExtendOne(c);
            }
        }

        /// <summary>
        /// Extend the tree with a single character.
        /// </summary>
        public void ExtendOne(char c) {
            _text.Add(c);
            _pos = _text.Count - 1;
            _needSLink = 0;
            _remainder++;

            PropagateAddition(c);
        }

        /// <summary>
        /// Mark the source text as completed by adding a `NUL` character to the end.
        /// </summary>
        public void Terminate() {
            ExtendOne('\0');
        }

        /// <summary>
        /// Find the index in `_tree` of a node matching the given pattern. Returns -1 if the pattern is not found.
        /// </summary>
        private int FindNodeIndex(string pattern, out int remains)
        {
            remains = 0;
            if (pattern == null) return -1;
            if (pattern == "") return 0;

            var strIdx = 0;
            var strEnd = pattern.Length;

            var nodeIndex = _root;
            var node = _tree[nodeIndex];
            var edgeText = NodeText(node);
            var edgeIdx = 0;

            while (strIdx < strEnd) {
                var c = pattern[strIdx];
                // walk through the current edge
                if (edgeIdx < edgeText.Length) {
                    if (edgeText[edgeIdx] != c) return -1;
                    edgeIdx++;
                    strIdx++;
                    continue;
                }

                // step next
                if (!node.Next.Contains(c)) return -1;
                nodeIndex = node.Next[c];
                node = _tree[node.Next[c]];
                edgeText = NodeText(node);
                edgeIdx = 0;
            }

            remains = (edgeText.Length - edgeIdx);
            return nodeIndex;
        }

        private void PropagateAddition(char c)
        {
            while (_remainder > 0)
            {
                if (_activeLength == 0) _activeEdge = _pos;

                if (_tree[_activeNode].Next[ActiveEdge()] == 0)
                {
                    var leaf = AddNode(_pos, Infinity);
                    _tree[_activeNode].Next[ActiveEdge()] = leaf;
                    AddSuffixLink(_activeNode);
                }
                else
                {
                    var next = _tree[_activeNode].Next[ActiveEdge()];
                    if (CanWalkDown(next)) continue;

                    if (_text[_tree[next].Start + _activeLength] == c)
                    {
                        _activeLength++;
                        AddSuffixLink(_activeNode);
                        break;
                    }

                    SplitPrefix(c, next);
                }

                _remainder--;
                if (_activeNode == _root && _activeLength > 0)
                {
                    _activeLength--;
                    _activeEdge = (_pos - _remainder) + 1;
                }
                else
                {
                    _activeNode = _tree[_activeNode].SuffixLink;
                }
            }
        }

        private void SplitPrefix(char c, int next)
        {
            var split = AddNode(_tree[next].Start, _tree[next].Start + _activeLength);
            _tree[_activeNode].Next[ActiveEdge()] = split;
            var leaf = AddNode(_pos, Infinity);
            _tree[split].Next[c] = leaf;
            _tree[next].Start += _activeLength;
            _tree[split].Next[_text[_tree[next].Start]] = next;
            AddSuffixLink(split);
        }

        private int AddNode(int start, int end) {
            var node = new SuffixNode{
                Start = start,
                End = end,
                SuffixLink = 0
            };

            _tree.Add(node);
            return _tree.Count - 1;
        }

        private char ActiveEdge() => _text[_activeEdge];
        private void AddSuffixLink(int node) {
            if (_needSLink > 0) _tree[_needSLink].SuffixLink = node;
            _needSLink = node;
        }

        private bool CanWalkDown(int nodeIdx) {
            var nodeLength = _tree[nodeIdx].EdgeLength(_pos);
            if (_activeLength < nodeLength) return false;

            _activeEdge += nodeLength;
            _activeLength -= nodeLength;
            _activeNode = nodeIdx;
            return true;
        }

    }
}