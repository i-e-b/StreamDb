using System.IO;
using System.Text;

namespace StreamDb.Internal.Support
{
    public class ByteString : IStreamSerialisable {
        private string _str;

        public static ByteString Wrap(string str) { return new ByteString{_str = str }; }

        /// <inheritdoc />
        public byte[] ToBytes() {
            if (_str == null) return new byte[0];
            return Encoding.UTF8?.GetBytes(_str) ?? new byte[0];
        }

        /// <inheritdoc />
        public void FromBytes(Stream source) {
            if (source == null) return;
            var bytes = new byte[source.RemainingLength()];
            source.Read(bytes, 0, source.RemainingLength());
            _str = Encoding.UTF8?.GetString(bytes);
        }

        public static implicit operator ByteString(string other){ return Wrap(other); }
        public static explicit operator string(ByteString other){ return other?._str; }
        public override string ToString() { return _str ?? ""; }
    }
}