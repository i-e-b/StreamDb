using System;
using System.IO;
using NUnit.Framework;
using StreamDb.Internal.Support;

// ReSharper disable PossibleNullReferenceException

namespace StreamDb.Tests
{
    [TestFixture]
    public class BasicTests
    {
        [Test]
        public void can_create_a_new_blank_database_in_a_stream (){
            using (var ms = new MemoryStream())
            {
                var subject = Database.TryConnect(ms);

                Console.WriteLine(DataDiagnosis.StreamToHex(ms));

                Assert.That(ms.Length, Is.GreaterThan(0), "Stream was not populated");
            }
        }

        [Test]
        public void can_open_an_existing_database_from_a_stream (){
            Assert.Fail("NYI");
        }

        [Test]
        public void database_can_be_accessed_with_a_readonly_stream ()
        {
            Assert.Fail("NYI");
        }

        [Test]
        public void trying_to_open_a_damaged_stream_gives_a_failure_result (){
            // at which point, you'd have a 'recover' method to call
            Assert.Fail("NYI");
        }

    }
}
