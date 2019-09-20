using System;

namespace StreamDb
{
    public class SerialGuid : IByteSerialisable {
        internal Guid _guid;
        public static SerialGuid Wrap(Guid g) { return new SerialGuid { _guid = g }; }
        
        public static implicit operator SerialGuid(Guid other){ return Wrap(other); }
        public static explicit operator Guid(SerialGuid other){ return other?._guid ?? Guid.Empty; }
        public byte[] ToBytes() { return _guid.ToByteArray(); }
        public void FromBytes(byte[] source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            _guid = new Guid(source);
        }
    }
}