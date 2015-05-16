// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   The transaction extensions.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace FabricTableService.Journal
{
    using System;
    using System.Collections.Concurrent;
    using System.Fabric.Replication;

    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// The transaction extensions.
    /// </summary>
    internal static class TransactionExtensions
    {
        /// <summary>
        /// The get transaction for type.
        /// </summary>
        private static readonly ConcurrentDictionary<Type, Func<ITransaction, Transaction>> getTransactionForType =
            new ConcurrentDictionary<Type, Func<ITransaction, Transaction>>();
        
        public static Transaction GetTransaction(this ITransaction tx)
        {
            // Note: this is ugly, but in order to co-operate with System.Fabric.Data's transactions, this is the cleanest way.
            var getTransaction = getTransactionForType.GetOrAdd(
                       tx.GetType(), 
                       type =>
                       {
                           var property = type.GetProperty("Transaction");
                           var getMethod = property.GetGetMethod();
                           return ttx => (Transaction)getMethod.Invoke(ttx, new object[0]);
                       });

            return getTransaction(tx);
        }
    }
}
