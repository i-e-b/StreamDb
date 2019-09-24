using System.IO;
using System.Text;
using NUnit.Framework;
using StreamDb.Internal;
// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class PageTableTests {

        [Test]
        public void create_page_table_in_stream () {
            var ms = new MemoryStream();
            var subject = new PageTable(ms);

            Assert.That(ms.Length, Is.EqualTo(Page.PageRawSize * 2), "Unexpected empty DB size");

            var page0 = subject.GetPage(0);
            Assert.That(page0, Is.Not.Null, "Failed to read root page");
            Assert.That(page0.ValidateCrc(), Is.True, "Root page CRC is wrong");

            var page1 = subject.GetPage(1);
            Assert.That(page1, Is.Not.Null, "Failed to read first index page");
            Assert.That(page1.ValidateCrc(), Is.True, "First index page CRC is wrong");
        }

        [Test]
        public void can_allocate_pages() {
            var ms = new MemoryStream();
            var subject = new PageTable(ms);

            var page_n = subject.GetFreePage();
            Assert.That(page_n, Is.Not.Null, "Page not provided");
            Assert.That(page_n.RootPageId, Is.EqualTo(2), "Unexpected page index");
        }

        [Test]
        public void can_read_a_page_table_from_a_stream () {
            byte[] raw;
            int pageId;
            var sample = Encoding.UTF8.GetBytes("Hello, world");

            using (var source = new MemoryStream()){
                var creator = new PageTable(source);
                var testPage = creator.GetFreePage();
                

                pageId = testPage.RootPageId;
                testPage.Write(sample, 0, 0, sample.Length);
                testPage.UpdateCRC();
                creator.CommitPage(testPage);

                source.Seek(0, SeekOrigin.Begin);
                raw = source.ToArray();
            }

            using (var stored = new MemoryStream(raw)) {
                var reader = new PageTable(stored);

                var resultPage = reader.GetPage(pageId);

                Assert.That(resultPage, Is.Not.Null, "Failed to read result page");
                
                var result = new byte[sample.Length];
                resultPage.Read(result, 0, 0, result.Length);

                var finalStr = Encoding.UTF8.GetString(result);
                Assert.That(finalStr, Is.EqualTo("Hello, world"));
            }
        }
    }
}