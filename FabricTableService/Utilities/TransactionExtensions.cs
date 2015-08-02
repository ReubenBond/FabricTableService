// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   The transaction extensions.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace FabricTableService.Utilities
{
    using System;
    using System.Collections.Concurrent;
    using System.Fabric.Replication;
    using System.Reflection;

    using Microsoft.ServiceFabric.Data;

    using Sigil;

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
                    var method = Emit<Func<ITransaction, Transaction>>.NewDynamicMethod(type.Name + "_GetTransaction");

                    var property = type.GetProperty(
                        "Transaction",
                        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic
                        | BindingFlags.FlattenHierarchy);
                    var getMethod = property.GetGetMethod();
                    method.LoadArgument(0);
                    method.CastClass(type);
                    method.Call(getMethod);
                    method.Return();
                    return method.CreateDelegate();
                });

            return getTransaction(tx);
        }
    }
}
