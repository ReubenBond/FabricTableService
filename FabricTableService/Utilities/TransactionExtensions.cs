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
    using System.Reflection.Emit;

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
            // Note: this is ugly, but this is currently the cleanest way to co-operate with System.Fabric.Data's transactions.
            var getTransaction = GetTransactionForType.GetOrAdd(
                tx.GetType(),
                type =>
                {
                    // Find the transaction property and its getter.
                    var property = type.GetProperty(
                           "Transaction",
                           BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic
                           | BindingFlags.FlattenHierarchy);
                    var getMethod = property.GetGetMethod();

                    // Create a method to retrieve the transaction property given an instance of this type.
                    var method = new DynamicMethod(
                        type.Name + "_GetTransaction",
                        typeof(Transaction),
                        new[] { typeof(ITransaction) },
                        typeof(TransactionExtensions).Module,
                        skipVisibility: true);

                    // Emit IL to return the value of the Transaction property.
                    var emitter = method.GetILGenerator();
                    emitter.Emit(OpCodes.Ldarg_0);
                    emitter.Emit(OpCodes.Castclass, type);
                    emitter.EmitCall(OpCodes.Call, getMethod, null);
                    emitter.Emit(OpCodes.Ret);

                    var result = method.CreateDelegate(typeof(Func<ITransaction, Transaction>));
                    return (Func<ITransaction, Transaction>)result;
                });

            return getTransaction(tx);
        }
    }
}
