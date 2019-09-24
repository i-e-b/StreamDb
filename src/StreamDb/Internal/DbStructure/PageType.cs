namespace StreamDb.Internal.DbStructure
{
    /// <summary>
    /// What the page represents
    /// </summary>
    public enum PageType : byte
    {
        /// <summary> The page type is not valid. This page was never correctly written </summary>
        Invalid = 0,
        
        /// <summary> Marker for a free page </summary>
        Free = 0xC0,

        /// <summary> This page is part of the index structure </summary>
        Index = 0x07,

        /// <summary> This page used to be an index page, but has now been freed </summary>
        FreeIndex = Free + Index, // 0xC7
        
        /// <summary> This page stores user data </summary>
        Data = 0x1C,
        
        /// <summary> This page used to store user data, but has now been freed </summary>
        FreeData = Free + Data, // 0xDC
        
        /// <summary>
        /// This is the single root page at index zero
        /// </summary>
        Root = 0x55
    }
}