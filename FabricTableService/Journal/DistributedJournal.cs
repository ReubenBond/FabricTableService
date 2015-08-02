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

    using ESENT = Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// The distributed journal.
    /// </summary>
    public partial class DistributedJournal<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// The table.
        /// </summary>
        private PersistentTablePool<TKey, TValue> tables;

        private readonly ConcurrentDictionary<long, OperationContext> inProgressOperations =
            new ConcurrentDictionary<long, OperationContext>();

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
            var initialValue = this.GetValue(key);
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

        public bool TryRemove(ITransaction tx, TKey key, long version = -1)
        {
            var transaction = tx.GetTransaction();

            var id = Interlocked.Increment(ref this.operationNumber);
            Operation undo;
            var initialValue = this.GetValue(key);
            if (initialValue == null)
            {
                undo = new RemoveOperation { Key = key, Id = id };
            }
            else
            {
                undo = new SetOperation { Key = key, Value = initialValue, Id = id };
            }

            var redo = new RemoveOperation { Key = key, Id = id };

            return this.PerformOperation<bool>(id, transaction, undo, redo);
        }

        /// <summary>
        /// The get value.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public TValue GetValue(TKey key)
        {
            var table = this.tables.Take();
            try
            {
                using (var tx = new ESENT.Transaction(table.Session))
                {
                    ESENT.Api.JetSetSessionContext(table.Session, table.Context);
                    TValue result;
                    table.TryGetValue(key, out result);
                    tx.Commit(ESENT.CommitTransactionGrbit.None);
                    return result;
                }
            }
            finally
            {
                ESENT.Api.JetResetSessionContext(table.Session);
                this.tables.Return(table);
            }
        }

        /// <summary>
        /// Gets a range of values.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public IEnumerable<KeyValuePair<TKey, TValue>> GetRange(TKey minKey, TKey maxKey)
        {
            var table = this.tables.Take();
            try
            {
                using (var tx = new ESENT.Transaction(table.Session))
                {
                    ESENT.Api.JetSetSessionContext(table.Session, table.Context);
                    var range = table.GetRange(minKey, maxKey);
                    tx.Commit(ESENT.CommitTransactionGrbit.None);
                    return range;
                }
            }
            finally
            {
                ESENT.Api.JetResetSessionContext(table.Session);
                this.tables.Return(table);
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
            var table = this.tables.Take();
            try
            {
                using (var tx = new ESENT.Transaction(table.Session))
                {
                    ESENT.Api.JetSetSessionContext(table.Session, table.Context);
                    var range = table.GetRange(minKey, maxValues: maxResults);
                    tx.Commit(ESENT.CommitTransactionGrbit.None);
                    return range;
                }
            }
            finally
            {
                ESENT.Api.JetResetSessionContext(table.Session);
                this.tables.Return(table);
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
            var table = this.tables.Take();
            try
            {
                using (var tx = new ESENT.Transaction(table.Session))
                {
                    ESENT.Api.JetSetSessionContext(table.Session, table.Context);
                    var range = table.GetRange(upperBound: maxKey, maxValues: maxResults);
                    tx.Commit(ESENT.CommitTransactionGrbit.None);
                    return range;
                }
            }
            finally
            {
                ESENT.Api.JetResetSessionContext(table.Session);
                this.tables.Return(table);
            }
        }
    }
}
