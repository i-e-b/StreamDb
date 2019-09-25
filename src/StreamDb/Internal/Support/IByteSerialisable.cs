using JetBrains.Annotations;

namespace StreamDb.Internal.Support
{
    /// <summary>
    /// Interface for classes that can serialise to/from bytes
    /// </summary>
    public interface IByteSerialisable
    {
        /// <summary>
        /// Convert this instance to a byte array
        /// </summary>
        [NotNull]byte[] ToBytes();

        /// <summary>
        /// Populate from a byte array
        /// </summary>
        void FromBytes(byte[] source);
    }
}