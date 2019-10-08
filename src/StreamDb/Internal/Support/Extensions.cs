using System.IO;
using System.Text;

namespace StreamDb.Internal.Support
{
    public static class Extensions
    {

        public static string ToHexString(this Stream s)
        {
            if (s == null) return "<null>";
            s.Seek(0, SeekOrigin.Begin);
            var sb = new StringBuilder();
            int i;
            while ((i = s.ReadByte()) > -1)
            {
                sb.Append(i.ToString("x2"));
            }
            return sb.ToString();
        }

        public static string ToHexString(this byte[] b) {
            if (b == null) return "<null>";
            var sb = new StringBuilder();
            for (int i = 0; i < b.Length; i++) {
                sb.Append(b[i].ToString("x2"));
            }
            return sb.ToString();
        }

        public static int RemainingLength(this Stream s) {
            if (s == null) return 0;
            return (int)(s.Length - s.Position);
        }

        public static Stream Rewind(this Stream s) {
            if (s == null) return null;
            s.Seek(0, SeekOrigin.Begin);
            return s;
        }
    }
}