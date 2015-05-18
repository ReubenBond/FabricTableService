// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DistributedJournal.cs" company="">
//   
// </copyright>
// <summary>
//   The distributed journal.
// </summary>
// --------------------------------------------------------------------------------------------------------------------
namespace FabricTableService.Journal
{
    using System;
    using System.Collections.Concurrent;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.Threading.Tasks;

    using global::FabricTableService.Journal.Database;

    using Microsoft.Isam.Esent.Interop;
    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// The distributed journal.
    /// </summary>
    public partial class DistributedJournal<TKey, TValue>
    {
        /// <summary>
        /// The table.
        /// </summary>
        private PersistentTable<TKey, TValue> table;

        private readonly ConcurrentDictionary<long, DbTransaction> inFlightTransactions = new ConcurrentDictionary<long, DbTransaction>();

        private readonly OperationPump<object> operationPump = new OperationPump<object>();
        
        /// <summary>
        /// The set value.
        /// </summary>
        /// <param name="tx">
        /// The tx.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        public async Task SetValue(ITransaction tx, TKey key, TValue value)
        {
            var transaction = tx.GetTransaction();
            /*this.inFlightTransactions.TryAdd(
                transaction.Id,
                new DbTransaction
                {
                    DatabaseTransaction = new Transaction(this.table.Session),
                    ReplicatorTransaction = transaction
                });*/

            Operation undo, redo;
            var initialValue = await this.GetValue(key);
            if (initialValue == null)
            {
                undo = new RemoveOperation { Key = key };
            }
            else
            {
                undo = new SetOperation { Key = key, Value = initialValue };
            }

            if (value == null)
            {
                redo = new RemoveOperation { Key = key };
            }
            else
            {
                redo = new SetOperation { Key = key, Value = value };
            }

            transaction.AddOperation(undo.Serialize(), redo.Serialize(), null, this.Name);
        }

        /// <summary>
        /// The get value.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public async Task<TValue> GetValue(TKey key)
        {
            return (TValue)await this.operationPump.Invoke(
                () =>
                {
                    TValue result;
                    this.table.TryGetValue(key, out result);
                    return result;
                });
        }

        internal struct DbTransaction
        {
            public System.Fabric.Replication.Transaction ReplicatorTransaction { get; set; }
            public Microsoft.Isam.Esent.Interop.Transaction DatabaseTransaction { get; set; }
        }
    }
}
