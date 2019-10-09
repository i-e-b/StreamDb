using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using JetBrains.Annotations;

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

        [NotNull]private static readonly Random rnd = new Random();  

        /// <summary>
        /// Randomise the order of the supplied list.
        /// </summary>
        public static IList<T> Shuffle<T>(this IList<T> list)  
        {  
            if (list == null) return null;
            var n = list.Count;  
            while (n > 1) {  
                n--;  
                var k = rnd.Next(n + 1);  
                var value = list[k];  
                list[k] = list[n];  
                list[n] = value;  
            }
            return list;
        }
    }
}