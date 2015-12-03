// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   The transaction extensions.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace FabricTableService.Utilities
{
    using Microsoft.ServiceFabric.Replicator;
    using Microsoft.ServiceFabric.Data;
    
    /// <summary>
    /// The transaction extensions.
    /// </summary>
    internal static class TransactionExtensions
    {
        public static Transaction GetTransaction(this ITransaction tx)
        {
            return (Transaction)tx;
        }
    }
}
