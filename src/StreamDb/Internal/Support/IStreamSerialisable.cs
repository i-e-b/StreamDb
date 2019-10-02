using System.IO;
using JetBrains.Annotations;

namespace StreamDb.Internal.Support
{
    /// <summary>
    /// Interface for classes that can serialise to/from bytes
    /// </summary>
    public interface IStreamSerialisable
    {
        /// <summary>
        /// Convert this instance to a byte stream
        /// </summary>
        [NotNull]Stream Freeze();

        /// <summary>
        /// Populate from a byte stream source
        /// </summary>
        void Defrost(Stream source);
    }
}