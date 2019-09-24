using System.IO;
using System.Text;

namespace StreamDb.Internal.Support
{
    public static class DataDiagnosis
    {

        public static string StreamToHex(Stream s)
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
    }
}