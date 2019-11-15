using NUnit.Framework;
using StreamDb.Internal.DbStructure;

// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class PageDataTests{

        [Test]
        public void simple_page_fields_round_trip () {
            var subject = new BasicPage(0);

            Assert.That(subject.DataLength, Is.EqualTo(0), "Unexpected default length");
            Assert.That(subject.PrevPageId, Is.EqualTo(-1), "Unexpected default page ID");

            subject.Write(new byte[] { 1, 2, 3, 4, 5, 6 }, 0, 0, 6);
            subject.PrevPageId = 32790;

            Assert.That(subject.DataLength, Is.EqualTo(6), "Unexpected modified length");
            Assert.That(subject.PrevPageId, Is.EqualTo(32790), "Unexpected modified page ID");
        }
        
        [Test]
        public void simple_page_crc_checks_work () {
            var subject = new BasicPage(0)
            {
                DataLength = 0,
                PrevPageId = 20,
            };


            subject.Write(new byte[] { 1, 2, 3, 4, 5, 6 }, 0, 0, 6);
            subject.UpdateCRC();

            Assert.That(subject.CrcHash, Is.Not.Zero, "CRC did not update");

            Assert.That(subject.ValidateCrc(), Is.True, "CRC check failed, but should have passed");

            subject.PrevPageId = 30;
            Assert.That(subject.ValidateCrc(), Is.False, "CRC check passed, but should have failed");
            
            subject.UpdateCRC();
            Assert.That(subject.ValidateCrc(), Is.True, "CRC check failed, but should have passed");
            subject.Write(new byte[] { 1, 2, 99, 4, 5, 6 }, 0, 0, 6);
            Assert.That(subject.ValidateCrc(), Is.False, "CRC check passed, but should have failed");
        }
    }
}