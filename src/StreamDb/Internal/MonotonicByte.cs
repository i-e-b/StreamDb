using System;

namespace StreamDb.Internal
{
    public class MonotonicByte : PartiallyOrdered, IByteSerialisable {

        private byte _value = 0;

        public int Value { get { return _value; } }

        /// <summary> Start a new counter on zero </summary>
        public MonotonicByte() { }

        /// <summary> Start a new counter with a given value </summary>
        public MonotonicByte(int value) { unchecked { _value = (byte)value; } }

        /// <summary>
        /// Advance the counter one position
        /// </summary>
        public byte Increment() {
            unchecked{ _value++; }
            return _value;
        }

        /// <inheritdoc />
        public byte[] ToBytes() { return new[] { _value }; }

        /// <inheritdoc />
        public void FromBytes(byte[] source) { if (source == null || source.Length < 1) throw new Exception("Invalid source"); _value = source[0]; }

        public override int CompareTo(object obj)
        {
            if (ReferenceEquals(this, obj)) return 0;
            if (ReferenceEquals(null, obj)) return 1;
            if (!(obj is MonotonicByte other)) return 1;

            var a = _value;
            var b = other._value;

            var diff = Math.Abs(b - a);
            var native = a.CompareTo(b);
            return (diff > 63) ? 1 - native : native;
        }

        public override int GetHashCode() { return 0x1437584b; }

        /// <summary>
        /// Get a new counter that is one version ahead of this one
        /// </summary>
        public MonotonicByte GetNext()
        {
            var next = new MonotonicByte(_value);
            next.Increment();
            return next;
        }
    }
}