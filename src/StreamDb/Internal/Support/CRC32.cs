using JetBrains.Annotations;

namespace StreamDb.Internal.Support
{
    public static class Crc32
    {
        public const uint DefaultPolynomial = 0xedb88320;
        public const uint DefaultSeed = 0xffffffff;

        [NotNull]private static readonly uint[] defaultTable;

        static Crc32()
        {
            var createTable = new uint[256];
            for (int i = 0; i < 256; i++)
            {
                var entry = (uint)i;
                for (int j = 0; j < 8; j++) entry = (entry & 1) == 1 ? (entry >> 1) ^ DefaultPolynomial : entry >> 1;
                createTable[i] = entry;
            }

            defaultTable = createTable;
        }

        /// <summary>
        /// Compute the CRC for 
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public static uint Compute(byte[] buffer)
        {
            if (buffer == null) return 0;
            var crc = DefaultSeed;
            foreach (var b in buffer)
            {
                crc = (crc >> 8) ^ defaultTable[b ^ (crc & 0xff)];
            }
            return ~crc;
        }
    }
}
