using System;
using NUnit.Framework;
using StreamDb.Internal;

namespace StreamDb.Tests
{
    [TestFixture]
    public class MonotonicByteTests
    {
        [Test]
        public void monotonic_bytes_can_loop () {
            var subject = new MonotonicByte();
            for (int i = 0; i < 384; i++)
            {
                subject.Increment();
            }

            Console.WriteLine(subject.Value + " (expecting 128)");
            Assert.That(subject.Value, Is.LessThan(384));
        }

        [Test]
        public void an_incremented_value_is_always_considered_greater_than_its_source()
        {

            var lower = new MonotonicByte();
            var upper = new MonotonicByte(1);

            for (int r = 0; r < 63; r++) // drift range (0..64, we start at drift=1)
            {
                for (int i = 0; i < 512; i++) // cycling
                {
                    Assert.That(lower < upper, Is.True, $"Drift = {r}; Compared {lower.Value} < {upper.Value} incorrectly");
                    lower.Increment();
                    upper.Increment();
                }
                upper.Increment();
            }
        }

        [Test]
        public void can_be_serialised_correctly()
        {
            var source = new MonotonicByte(140);
            var bytes = source.ToBytes();
            var dest = new MonotonicByte();
            dest.FromBytes(bytes);

            Assert.That(source == dest, Is.True);
        }

    }
}