namespace StreamDb.Internal.DbStructure
{
    /// <summary>
    /// What the page represents
    /// </summary>
    public enum PageType : byte
    {
        // bottom 5 bytes for type, top 3 for flags

        /// <summary> The page type is not valid. This page was never correctly written </summary>
        Invalid = 0,
        
        /// <summary>
        /// This is the single root page at index zero. It cannot be freed.
        /// </summary>
        Root = 0x55,    // 010 10101
        
        /// <summary> This page is part of the index structure </summary>
        Index = 0x07,       // 00111

        /// <summary> This page is part of the free table </summary>
        ExpiredList = 0x03, // 10011
        
        /// <summary> This page is part of the path lookup structure </summary>
        PathLookup = 0x19,  // 11001
        
        /// <summary> This page stores user data </summary>
        Data = 0x1C,        // 11100
        

        /// <summary> Marker for a free page </summary>
        Free = 0xC0,    // 110 00000

        /// <summary> This page used to be part of the free table, but has now been freed </summary>
        FreeExpiredList = Free + ExpiredList,

        /// <summary> This page used to be an index page, but has now been freed </summary>
        FreeIndex = Free + Index, // 0xC7
        
        /// <summary> This page used to store user data, but has now been freed </summary>
        FreeData = Free + Data, // 0xDC

        /// <summary> This page used to store part of the path lookup structure, but has now been freed </summary>
        FreePathLookup = Free + PathLookup
    }
}