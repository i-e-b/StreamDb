using System;
using System.IO;

namespace StreamDb.Internal.Support
{
    public struct MonotonicByte :  IStreamSerialisable {

        private byte _value;

        public int Value { get { return _value; } }

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
        public Stream Freeze() {
            return new MemoryStream(new[] { _value });
        }

        /// <inheritdoc />
        public void Defrost(Stream source) {
            if (source == null || source.Length < 1) throw new Exception("Invalid source");
            _value = (byte)source.ReadByte();
        }
#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
        public static int CompareTo(MonotonicByte x, object y) { return x.CompareTo(y); }
        public static bool operator  < (MonotonicByte x, MonotonicByte y) { return CompareTo(x, y)  < 0; }
        public static bool operator  > (MonotonicByte x, MonotonicByte y) { return CompareTo(x, y)  > 0; }
        public static bool operator <= (MonotonicByte x, MonotonicByte y) { return CompareTo(x, y) <= 0; }
        public static bool operator >= (MonotonicByte x, MonotonicByte y) { return CompareTo(x, y) >= 0; }
        public static bool operator == (MonotonicByte x, MonotonicByte y) { return CompareTo(x, y) == 0; }
        public static bool operator != (MonotonicByte x, MonotonicByte y) { return CompareTo(x, y) != 0; }
        public bool Equals(MonotonicByte x)    { return CompareTo(this, x) == 0; }
#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            return (obj is PartiallyOrdered ordered) && (CompareTo(this, ordered) == 0);
        }

        public int CompareTo(object obj)
        {
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