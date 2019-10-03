using System;
using System.IO;

namespace StreamDb.Internal.Support
{
    public class SerialGuid : PartiallyOrdered, IStreamSerialisable {
        public Guid Value;
        public static SerialGuid Wrap(Guid g) { return new SerialGuid { Value = g }; }
        
        public static implicit operator SerialGuid(Guid other){ return Wrap(other); }
        public static explicit operator Guid(SerialGuid other){ return other?.Value ?? Guid.Empty; }
        public Stream Freeze() { return new MemoryStream(Value.ToByteArray()); }
        public void Defrost(Stream source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            
            var buf = new byte[16];
            if (source.Read(buf, 0, 16) != 16) throw new Exception("Source stream was too short to read GUID");
            Value = new Guid(buf);
        }

        /// <inheritdoc />
        public override int CompareTo(object obj)
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