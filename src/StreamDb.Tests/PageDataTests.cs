using System;
using NUnit.Framework;
using StreamDb.Internal;
using StreamDb.Internal.DbStructure;

// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class PageDataTests{

        [Test]
        public void crc_checks_work () {
            var subject = new Page
            {
                DocumentId = Guid.NewGuid(),
                DocumentSequence = 12,
                NextPageId = -1,
                FirstPageId = 25,
                PrevPageId = 20,
                PageType = PageType.Data
            };

            subject.UpdateCRC();

            Assert.That(subject.CrcHash, Is.Not.Zero, "CRC did not update");

            Assert.That(subject.ValidateCrc(), Is.True, "CRC check failed, but should have passed");

            subject.DocumentId = Guid.NewGuid();
            Assert.That(subject.ValidateCrc(), Is.False, "CRC check passed, but should have failed");
        }
    }
}