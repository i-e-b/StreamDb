using NUnit.Framework;

namespace StreamDb.Tests
{
    [TestFixture]
    public class PersistentReverseTrie {

        [Test]
        public void construction () {
            // Idea:
            // We build a trie (wide, not ternary) using backward links only (an odd form of a persistent trie [ https://www.geeksforgeeks.org/persistent-trie-set-1-introduction/ ])
            // This is our permanent and growable data structure.
            // We then make a separate 'forward links index' as a NON-STORED in-memory cache.
            // This cache gets invalidated on every write, and rebuilt during a read if the invalidation flag is set.
            //
            // Note: using a normal persistent trie might cause an issue, as it duplicates on prefix reuse.

            // TODO: diagram and build a prototype
            Assert.Inconclusive();
        }
    }
}