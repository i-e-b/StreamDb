using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;

namespace StreamDb.Tests
{
    [TestFixture]
    public class UkkonenSuffixTreeTests{
    
        [Test]
        public void construction_from_text ()
        {
            // https://en.wikipedia.org/wiki/Ukkonen%27s_algorithm
            // https://stackoverflow.com/questions/9452701/ukkonens-suffix-tree-algorithm-in-plain-english/9513423#9513423
            // https://marknelson.us/posts/1996/08/01/suffix-trees.html
            // http://programmerspatch.blogspot.com/2013/02/ukkonens-suffix-tree-algorithm.html
            // http://brenden.github.io/ukkonen-animation/

            var subject = new SuffixTree();
            subject.Extend("how much wood would a wood chuck chuck if a wood chuck could chuck wood");

            var desc = subject.TreeDescription();
            Console.WriteLine(desc);
        }
    }

    public class SuffixNode {
        public int Start;
        public int End;
        public int SuffixLink;
        public Map<int,int> Next;

        public SuffixNode()
        {
            Next = new Map<int, int>(); //int[SuffixTree.SymbolCount];
        }

        public int EdgeLength(int position) => Math.Min(End, position + 1) - Start;
    }

    public class Map<TIdx, TVal>
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

        public Map() { _data = new Dictionary<TIdx, TVal>(); }
    }

    public class SuffixTree
    {
        public const int Infinity = 1 << 28; // Special value marking 'until end', sometimes '#' in papers.
        public const int SymbolCount = 256; // also known as 'Alphabet Size' in papers.

        int _root, _lastAdded, _pos, _needSLink, _remainder, _activeNode, _activeEdge, _activeLength;

        readonly List<SuffixNode> _tree;
        List<char> _text;

        public SuffixTree()
        {
            _tree = new List<SuffixNode>();
            _text = new List<char>();

            _needSLink = 0;
            _lastAdded = 0;
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

            DescribeNodeRecursive(sb, _root);

            return sb.ToString();
        }

        private void DescribeNodeRecursive(StringBuilder sb, int idx)
        {
            sb.Append("(");
            var node = _tree[idx];
            var stop = Math.Min(node.End, _text.Count);
            if (node.Start == stop) sb.Append("^");
            for (int i = node.Start; i < stop; i++)
            {
                sb.Append(_text[i]);
            }

            sb.Append(" -> ");

            foreach (var nextChar in node.Next.Keys())
            {
                sb.Append((char) nextChar);
                DescribeNodeRecursive(sb, node.Next[nextChar]);
                sb.Append("\r\n");
            }

            sb.Append(") ");
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

            while (_remainder > 0)
            {
                if (_activeLength == 0) _activeEdge = _pos;

                if (_tree[_activeNode].Next[ActiveEdge()] == 0) {
                    var leaf = AddNode(_pos, Infinity);
                    _tree[_activeNode].Next[ActiveEdge()] = leaf;
                    AddSuffixLink(_activeNode);
                }
                else {
                    var next = _tree[_activeNode].Next[ActiveEdge()];
                    if (CanWalkDown(next)) continue;

                    if (_text[_tree[next].Start + _activeLength] == c) {
                        _activeLength++;
                        AddSuffixLink(_activeNode);
                        break;
                    }
                    SplitPrefix(c, next);
                }

                _remainder--;
                if (_activeNode == _root && _activeLength > 0) {
                    _activeLength--;
                    _activeEdge = _pos - _remainder + 1;
                } else {
                    _activeNode = (_tree[_activeNode].SuffixLink > 0)
                        ? _tree[_activeNode].SuffixLink
                        : _root;
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