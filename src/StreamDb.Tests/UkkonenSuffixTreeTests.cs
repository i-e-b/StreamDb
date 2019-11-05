using System;
using System.Diagnostics;
using System.Linq;
using NUnit.Framework;
using StreamDb.Internal.Search;

namespace StreamDb.Tests
{
    [TestFixture]
    public class UkkonenSuffixTreeTests
    {
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

        [Test]
        public void build_for_large_binary () {
            var sw = new Stopwatch();

            sw.Restart();
            var data = new byte[32768];
            var rnd = new Random();
            rnd.NextBytes(data);
            sw.Stop();
            Console.WriteLine($"Data generation took {sw.Elapsed} for 32KB");

            sw.Restart();
            var subject = new SuffixTree();
            subject.Extend(data);
            subject.Terminate();
            sw.Stop();
            Console.WriteLine($"Building suffix tree took {sw.Elapsed}");

            
            sw.Restart();
            var positions = subject.FindAll("x").OrderBy(n=>n).ToList();
            var result = string.Join(",", positions);
            sw.Stop();
            Console.WriteLine($"Querying suffix tree took {sw.Elapsed}");
            
            Console.WriteLine($"\r\n'x' found in these {positions.Count} positions: {result}");
        }
    }

}