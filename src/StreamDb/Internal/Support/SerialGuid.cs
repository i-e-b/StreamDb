using System;
using System.IO;

namespace StreamDb.Internal.Support
{
    /// <summary>
    /// GUID that is stream serialisable and ordered
    /// </summary>
    public class SerialGuid : PartiallyOrdered, IStreamSerialisable {
        /// <summary>
        /// GUID value
        /// </summary>
        public Guid Value;
        /// <summary>
        /// Wrap a guid in an orderable serialiser
        /// </summary>
        public static SerialGuid Wrap(Guid g) { return new SerialGuid { Value = g }; }
        
        /// <summary>
        /// Implicit conversion
        /// </summary>
        public static implicit operator SerialGuid(Guid other){ return Wrap(other); }
        /// <summary>
        /// Explicit conversion
        /// </summary>
        public static explicit operator Guid(SerialGuid? other){ return other?.Value ?? Guid.Empty; }
        /// <summary>
        /// Serialise to a stream
        /// </summary>
        public Stream Freeze() { return new MemoryStream(Value.ToByteArray()); }
        /// <summary>
        /// Recover from a stream
        /// </summary>
        public void Defrost(Stream source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            
            var buf = new byte[16];
            var actual = source.Read(buf, 0, 16);
            if (actual != 16) throw new Exception($"Source stream was too short to read GUID (expected 16, got {actual})");
            Value = new Guid(buf);
        }

        /// <inheritdoc />
        public override int CompareTo(object? obj)
        {
            if (!(obj is SerialGuid other)) return -1;
            return Value.CompareTo(other.Value);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            // ReSharper disable once NonReadonlyMemberInGetHashCode
            return Value.GetHashCode();
        }
    }
}