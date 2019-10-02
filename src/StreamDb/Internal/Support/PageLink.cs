using JetBrains.Annotations;

namespace StreamDb.Internal.Support
{
    /// <summary>
    /// A versioned link to a page chain.
    /// These should always be used in pairs. The most recent is read,
    /// the older is overwritten
    /// </summary>
    /// <remarks>9 bytes</remarks>
    public class PageLink {
        /// <summary>
        /// Version of this link. Always use the latest link whose page has a valid CRC
        /// </summary>
        public MonotonicByte Version { get; set; }

        /// <summary>
        /// End of the page chain (for writing).
        /// That page will have a link back to the start (for reading)
        /// </summary>
        public int PageId { get; set; }

        public PageLink() { Version = new MonotonicByte(); }

        /// <summary>
        /// Return a link that is disabled
        /// </summary>
        [NotNull]public static PageLink InvalidLink()
        {
            return new PageLink { Version = new MonotonicByte(), PageId = -1 };
        }
    }
}