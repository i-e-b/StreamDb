using JetBrains.Annotations;

namespace StreamDb.Internal
{
    public static class Crc32
    {
        public const uint DefaultPolynomial = 0xedb88320;
        public const uint DefaultSeed = 0xffffffff;

        private static uint[] defaultTable;

        public static uint Compute(byte[] buffer)
        {
            if (buffer == null) return 0;
            return ~CalculateHash(InitializeTable(DefaultPolynomial), DefaultSeed, buffer, 0, buffer.Length);
        }

        public static uint Compute(uint seed, byte[] buffer)
        {
            if (buffer == null) return 0;
            return ~CalculateHash(InitializeTable(DefaultPolynomial), seed, buffer, 0, buffer.Length);
        }

        public static uint Compute(uint polynomial, uint seed, byte[] buffer)
        {
            if (buffer == null) return 0;
            return ~CalculateHash(InitializeTable(polynomial), seed, buffer, 0, buffer.Length);
        }

        [NotNull]private static uint[] InitializeTable(uint polynomial)
        {
            if (polynomial == DefaultPolynomial && defaultTable != null) return defaultTable;

            var createTable = new uint[256];
            for (int i = 0; i < 256; i++)
            {
                var entry = (uint)i;
                for (int j = 0; j < 8; j++) entry = (entry & 1) == 1 ? (entry >> 1) ^ polynomial : entry >> 1;
                createTable[i] = entry;
            }

            if (polynomial == DefaultPolynomial) defaultTable = createTable;
            return createTable;
        }

        private static uint CalculateHash([NotNull]uint[] table, uint seed, [NotNull]byte[] buffer, int start, int size)
        {
            var crc = seed;
            for (int i = start; i < size; i++)
                unchecked
                {
                    crc = (crc >> 8) ^ table[buffer[i] ^ crc & 0xff];
                }
            return crc;
        }

        public static byte[] UInt32ToBigEndianBytes(uint x)
        {
            return new[] {
                (byte)((x >> 24) & 0xff),
                (byte)((x >> 16) & 0xff),
                (byte)((x >> 8) & 0xff),
                (byte)(x & 0xff)
            };
        }
    }
}
