using System;

namespace StreamDb.Internal.Support
{
    public class SerialGuid : IStreamSerialisable {
        public Guid Value;
        public static SerialGuid Wrap(Guid g) { return new SerialGuid { Value = g }; }
        
        public static implicit operator SerialGuid(Guid other){ return Wrap(other); }
        public static explicit operator Guid(SerialGuid other){ return other?.Value ?? Guid.Empty; }
        public byte[] ToBytes() { return Value.ToByteArray(); }
        public void FromBytes(byte[] source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            Value = new Guid(source);
        }
    }
}