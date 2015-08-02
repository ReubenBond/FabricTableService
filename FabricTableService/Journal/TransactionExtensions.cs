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
    using System.Linq.Expressions;

    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// The transaction extensions.
    /// </summary>
    internal static class TransactionExtensions
    {
        /// <summary>
        /// The get transaction for type.
        /// </summary>
        private static readonly ConcurrentDictionary<Type, Func<ITransaction, Transaction>> GetTransactionForType =
            new ConcurrentDictionary<Type, Func<ITransaction, Transaction>>();
        
        public static Transaction GetTransaction(this ITransaction tx)
        {
            // Note: this is ugly, but in order to co-operate with System.Fabric.Data's transactions, this is the cleanest way.
            var getTransaction = GetTransactionForType.GetOrAdd(
                       tx.GetType(), 
                       type =>
                       {
                           var property = type.GetProperty("Transaction");
                           var getMethod = property.GetGetMethod();
                           var txArg = Expression.Parameter(typeof(ITransaction), "tx");
                           var lambda =
                               Expression.Lambda<Func<ITransaction, Transaction>>(
                                   Expression.Invoke(Expression.MakeMemberAccess(txArg, getMethod)),
                                   txArg);
                           return lambda.Compile();
                       });

            return getTransaction(tx);
        }
    }
}
