namespace FabricTableService.Utilities
{
    using Microsoft.IO;

    /// <summary>
    /// The memory stream manager.
    /// </summary>
    internal static class MemoryStreamManager
    {
        /// <summary>
        /// The memory stream pool.
        /// </summary>
        public static readonly RecyclableMemoryStreamManager Pool = new RecyclableMemoryStreamManager();
    }
}