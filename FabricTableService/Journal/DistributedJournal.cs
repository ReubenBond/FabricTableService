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
    using System.CodeDom;
    using System.Collections.Concurrent;
    using System.Fabric.Replication;
    using System.Threading;
    using System.Threading.Tasks;

    using global::FabricTableService.Journal.Database;

    using Microsoft.ServiceFabric.Data;
    using ESENT = Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// The distributed journal.
    /// </summary>
    public partial class DistributedJournal<TKey, TValue>
    {
        /// <summary>
        /// The table.
        /// </summary>
        private PersistentTablePool<TKey, TValue> tablePool;

        private readonly ConcurrentDictionary<long, OperationContext> inFlightOperations = new ConcurrentDictionary<long, OperationContext>();

        private readonly ConcurrentDictionary<long, ConcurrentQueue<long>> inFlightTransactions =
            new ConcurrentDictionary<long, ConcurrentQueue<long>>();

        private readonly OperationPump<object> operationPump = new OperationPump<object>();

        private long operationNumber;
        
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

            var id = Interlocked.Increment(ref this.operationNumber);

            Operation undo, redo;
            var initialValue = await this.GetValue(key);
            if (initialValue == null)
            {
                undo = new RemoveOperation { Key = key, Id = id };
            }
            else
            {
                undo = new SetOperation { Key = key, Value = initialValue, Id = id };
            }

            if (value == null)
            {
                redo = new RemoveOperation { Key = key, Id = id };
            }
            else
            {
                redo = new SetOperation { Key = key, Value = value, Id = id };
            }

            
            this.PerformOperation<object>(id, transaction, undo, redo);
        }

        public async Task<bool> TryRemove(ITransaction tx, TKey key, long version = -1)
        {
            var transaction = tx.GetTransaction();

            var id = Interlocked.Increment(ref this.operationNumber);
            Operation undo;
            var initialValue = await this.GetValue(key);
            if (initialValue == null)
            {
                undo = new RemoveOperation { Key = key, Id = id };
            }
            else
            {
                undo = new SetOperation { Key = key, Value = initialValue, Id = id };
            }

            var redo = new RemoveOperation { Key = key, Id = id };

            return this.PerformOperation<bool>(id, transaction,  undo, redo);
        }

        /// <summary>
        /// The get value.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public async Task<TValue> GetValue(TKey key)
        {
            var table = this.tablePool.Take();
            return (TValue)await this.operationPump.Invoke(
                () =>
                {
                    try
                    {
                        TValue result;
                        table.TryGetValue(key, out result);
                        return result;
                    }
                    finally
                    {
                        this.tablePool.Return(table);
                    }
                });
        }

        /// <summary>
        /// Gets a range of values.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public async Task<TValue> GetRange(TKey minKey, TKey maxKey)
        {
            var table = this.tablePool.Take();
            return (TValue)await this.operationPump.Invoke(
                () =>
                {
                    try
                    {
                        return table.GetRange(minKey, maxKey);
                    }
                    finally
                    {
                        this.tablePool.Return(table);
                    }
                });
        }

        /// <summary>
        /// Gets a range of values.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public async Task<TValue> GetGreaterThan(TKey minKey, long maxResults)
        {
            var table = this.tablePool.Take();
            return (TValue)await this.operationPump.Invoke(
                () =>
                {
                    try
                    {
                        return table.GetRange(minKey, maxValues: maxResults);
                    }
                    finally
                    {
                        this.tablePool.Return(table);
                    }
                });
        }

        /// <summary>
        /// Gets a range of values.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public async Task<TValue> GetLessThan(TKey maxKey, long maxResults)
        {
            var table = this.tablePool.Take();
            return
                (TValue)
                await this.operationPump.Invoke(
                    () =>
                    {
                        try
                        {
                            return table.GetRange(upperBound: maxKey, maxValues: maxResults);
                        }
                        finally
                        {
                            this.tablePool.Return(table);
                        }
                    });
        }

        private T PerformOperation<T>(long id, Transaction transaction, Operation undo, Operation redo)
        {
            T result;
            var table = this.tablePool.Take();
            var dbTransaction = new ESENT.Transaction(table.Session);
            try
            {
                // Apply initially on primary.
                result = (T)redo.Apply(commit: false, apply: true, transaction: dbTransaction, table: table);

                // Add the operation to the in-flight transactions collection so that we can retrieve it when it is committed.
                if (
                    !this.inFlightOperations.TryAdd(id,
                        new OperationContext
                        {
                            ReplicatorTransaction = transaction,
                            DatabaseTransaction = dbTransaction,
                            /* SuccessCallback = _ => result.TrySetResult(_),
                    FailureCallback = _ => result.TrySetException(_)*/
                        }))
                {
                    throw new InvalidOperationException(string.Format("Operation with id {0} already in-flight.", id));
                }

                // Add this operation to the transaction.
                var transactionOperations = this.inFlightTransactions.GetOrAdd(
                    transaction.Id,
                    _ => new ConcurrentQueue<long>());
                transactionOperations.Enqueue(id);

                transaction.AddOperation(undo.Serialize(), redo.Serialize(), null, this.Name);
            }
            catch
            {
                dbTransaction.Rollback();
                this.tablePool.Return(table);
                throw;
            }

            return result;
        }

        internal struct OperationContext
        {
            public Transaction ReplicatorTransaction { get; set; }
            public ESENT.Transaction DatabaseTransaction { get; set; }

            /// <summary>
            /// The operation to execute on successful completion.
            /// </summary>
            public Action<object> SuccessCallback { get; set; }

            /// <summary>
            /// The operation to execute on failure.
            /// </summary>
            public Action<Exception> FailureCallback { get; set; }

            public PersistentTable<TKey, TValue> Table { get; set; }
        }
    }
}
