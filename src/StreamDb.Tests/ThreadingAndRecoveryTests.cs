using NUnit.Framework;

namespace StreamDb.Tests
{
    [TestFixture]
    public class ThreadingAndRecoveryTests {
        [Test]
        public void crashing_half_way_through_a_write_results_in_no_document_added () {
            Assert.Inconclusive();
        }

        [Test]
        public void if_a_document_is_lost_we_can_recover_the_written_pages () {
            // Idea: when we allocate a page, or recover it from the free page list,
            // then we add it to the free page list with a 'journal' flag.
            // we can then wipe those when the write is good.
            Assert.Inconclusive();
        }

        [Test]
        public void when_a_document_crashes_during_an_update_the_previous_version_is_still_available () {
            Assert.Inconclusive();
        }

        [Test]
        public void writing_documents_in_multiple_threads_works_correctly () {
            // Note: the path index is probably trickiest
            Assert.Inconclusive();
        }

        [Test]
        public void reading_documents_in_multiple_threads_works_correctly () {
            Assert.Inconclusive();
        }
    }
}