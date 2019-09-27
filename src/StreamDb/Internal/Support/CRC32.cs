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

        // TODO: Look at https://github.com/stbrumme/crc32/blob/master/Crc32.cpp
        //        We should be able to get a much faster CRC
        public static uint Compute(byte[] buffer)
        {
            if (buffer == null) return 0;
            return ~CalculateHash(defaultTable, DefaultSeed, buffer, 0, buffer.Length);
        }

        private static uint CalculateHash([NotNull]uint[] table, uint seed, [NotNull]byte[] buffer, int start, int size)
        {
            var crc = seed;
            unchecked
            {
                for (int i = start; i < size; i++)
                {
                    crc = (crc >> 8) ^ table[buffer[i] ^ (crc & 0xff)];
                }
            }
            return crc;
        }
    }
}
