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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    using global::FabricTableService.Journal.Database;
    using global::FabricTableService.Utilities;

    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// The distributed journal.
    /// </summary>
    public partial class ReliableTable<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// The pool of tables.
        /// </summary>
        private PersistentTablePool<TKey, TValue> tables;

        /// <summary>
        /// Operations which are currently in-progress.
        /// </summary>
        private readonly ConcurrentDictionary<long, OperationContext> inProgressOperations =
            new ConcurrentDictionary<long, OperationContext>();

        /// <summary>
        /// The current operation number.
        /// </summary>
        private long operationNumber;

        /// <summary>
        /// Disposes this instance.
        /// </summary>
        public void Dispose()
        {
            ((IDisposable)this.tables).Dispose();
        }

        public Task Backup(string destination)
        {
            return this.tables.Backup(destination);
        }

        public Task Restore(string backupPath)
        {
            return this.tables.Restore(backupPath, this.tables.Directory);
        }

        public Task RestoreTo(string backupPath, string destination)
        {
            return this.tables.Restore(backupPath, destination);
        }

        /// <summary>
        /// The set value.
        /// </summary>
        /// <param name="tx">
        /// The tx.
        /// </param>
        /// <param name="key">
        /// The key.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        public void SetValue(ITransaction tx, TKey key, TValue value)
        {
            var transaction = tx.GetTransaction();

            var id = Interlocked.Increment(ref this.operationNumber);

            Operation undo, redo;
            var dbTransaction = this.tables.CreateTransaction();
            try
            {
                TValue initialValue;
                if (dbTransaction.Table.TryGetValue(key, out initialValue))
                {
                    undo = new SetOperation { Key = key, Value = initialValue, Id = id };
                }
                else
                {
                    undo = new RemoveOperation { Key = key, Id = id };
                }
            }
            catch
            {
                dbTransaction.Rollback();
                dbTransaction.Dispose();
                throw;
            }

            if (value == null)
            {
                redo = new RemoveOperation { Key = key, Id = id };
            }
            else
            {
                redo = new SetOperation { Key = key, Value = value, Id = id };
            }

            this.PerformOperation<object>(
                id,
                new OperationContext { DatabaseTransaction = dbTransaction, ReplicatorTransaction = transaction },
                undo,
                redo);
        }

        public ConditionalResult<TValue> TryRemove(ITransaction tx, TKey key, long version = -1)
        {
            var transaction = tx.GetTransaction();

            var id = Interlocked.Increment(ref this.operationNumber);
            Operation undo;
            var dbTransaction = this.tables.CreateTransaction();
            ConditionalResult<TValue> result;
            try
            {
                TValue initialValue;
                if (dbTransaction.Table.TryGetValue(key, out initialValue))
                {
                    undo = new SetOperation { Key = key, Value = initialValue, Id = id };
                    result = new ConditionalResult<TValue>(true, initialValue);
                }
                else
                {
                    undo = NopOperation.Instance;
                    result = new ConditionalResult<TValue>();
                }
            }
            catch
            {
                dbTransaction.Rollback();
                dbTransaction.Dispose();
                throw;
            }

            var redo = new RemoveOperation { Key = key, Id = id };

            this.PerformOperation<object>(
                id,
                new OperationContext { DatabaseTransaction = dbTransaction, ReplicatorTransaction = transaction },
                undo,
                redo);

            return result;
        }

        /// <summary>
        /// The get value.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public ConditionalResult<TValue> GetValue(ITransaction tx, TKey key)
        {
            var transaction = tx.GetTransaction();
            var id = Interlocked.Increment(ref this.operationNumber);
            return this.PerformOperation<ConditionalResult<TValue>>(
                id,
                new OperationContext
                {
                    DatabaseTransaction = this.tables.CreateTransaction(),
                    ReplicatorTransaction = transaction
                },
                NopOperation.Instance,
                new GetOperation { Key = key, Id = id });
        }

        /// <summary>
        /// Gets a range of values.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public IEnumerable<KeyValuePair<TKey, TValue>> GetRange(TKey minKey, TKey maxKey)
        {
            using (var tx = this.tables.CreateTransaction())
            {
                var range = tx.Table.GetRange(minKey, maxKey);
                tx.Commit();
                return range;
            }
        }

        /// <summary>
        /// Gets a range of values.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public IEnumerable<KeyValuePair<TKey, TValue>> GetGreaterThan(TKey minKey, long maxResults)
        {
            using (var tx = this.tables.CreateTransaction())
            {
                var range = tx.Table.GetRange(minKey, maxValues: maxResults);
                tx.Commit();
                return range;
            }
        }

        /// <summary>
        /// Gets a range of values.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public IEnumerable<KeyValuePair<TKey, TValue>> GetLessThan(TKey maxKey, long maxResults)
        {
            using (var tx = this.tables.CreateTransaction())
            {
                var range = tx.Table.GetRange(upperBound: maxKey, maxValues: maxResults);
                tx.Commit();
                return range;
            }
        }
    }
}