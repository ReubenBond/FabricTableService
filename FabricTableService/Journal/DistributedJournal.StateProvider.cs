// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   The distributed journal.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace FabricTableService.Journal
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Fabric;
    using System.Fabric.Replication;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;

    using global::FabricTableService.Journal.Database;
    using global::FabricTableService.Json;

    using Microsoft.ServiceFabric.Data;

    using Newtonsoft.Json;

    using Transaction = System.Fabric.Replication.Transaction;
    using TransactionBase = System.Fabric.Replication.TransactionBase;
    using ESENT = Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// The distributed journal.
    /// </summary>
    /// <typeparam name="TKey">
    /// The key type.
    /// </typeparam>
    /// <typeparam name="TValue">
    /// The value type.
    /// </typeparam>
    public partial class DistributedJournal<TKey, TValue> : IStateProvider2, IReliableState
    {
        /// <summary>
        /// The state replicator.
        /// </summary>
        private TransactionalReplicator replicator;

        /// <summary>
        /// The partition id.
        /// </summary>
        private string partitionId;

        /// <summary>
        /// Gets the name of this provider.
        /// </summary>
        public Uri Name { get; private set; }

        /// <summary>
        /// Gets the initialization context.
        /// </summary>
        public byte[] InitializationContext { get; private set; }

        /// <summary>
        /// The initialize.
        /// </summary>
        /// <param name="transactionalReplicator">
        /// The transactional replicator.
        /// </param>
        /// <param name="name">
        /// The name.
        /// </param>
        /// <param name="initializationContext">
        /// The initialization context.
        /// </param>
        /// <param name="stateProviderId">
        /// The state provider id.
        /// </param>
        void IStateProvider2.Initialize(
            TransactionalReplicator transactionalReplicator, 
            Uri name, 
            byte[] initializationContext, 
            Guid stateProviderId)
        {
            this.replicator = transactionalReplicator;
            this.partitionId = this.replicator.StatefulPartition.PartitionInfo.Id.ToString("N");
            this.InitializationContext = initializationContext;
            this.Name = name;
            var contextString = initializationContext == null
                                    ? string.Empty
                                    : string.Concat(initializationContext.Select(_ => $"{_:X}"));
            Trace.TraceInformation(
                "[" + this.partitionId + "] " + "Initialize({0}, {1}, {2})",
                name,
                contextString,
                stateProviderId);
        }

        /// <summary>
        /// The open async.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.OpenAsync()
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "OpenAsync()");

            var tableName = this.GetTableName();
            var workDirectory = this.replicator.InitializationParameters.CodePackageActivationContext.WorkDirectory;
            var databaseDirectory = Path.Combine(workDirectory, this.partitionId, "journal");
            Directory.CreateDirectory(databaseDirectory);
            this.tables = new PersistentTablePool<TKey, TValue>(databaseDirectory, "db.edb", tableName);
            this.tables.Initialize();
            return Task.FromResult(0);
        }

        /// <summary>
        /// Gets the table name.
        /// </summary>
        /// <returns></returns>
        private string GetTableName()
        {
            var tableName = this.Name.ToString().Substring(this.Name.Scheme.Length);
            return Regex.Replace(tableName, @"[^a-zA-Z0-9_\-]", string.Empty);
        }

        /// <summary>
        /// The close async.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.CloseAsync()
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "CloseAsync()");
            var tab = this.tables;
            if (tab != null)
            {
                ((IDisposable)tab).Dispose();
                this.tables = null;
            }

            return Task.FromResult(0);
        }

        /// <summary>
        /// The abort.
        /// </summary>
        void IStateProvider2.Abort()
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "Abort()");
        }

        /// <summary>
        /// The change role async.
        /// </summary>
        /// <param name="newRole">
        /// The new role.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.ChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "ChangeRoleAsync({0})", newRole);
            return Task.FromResult(0);
        }

        /// <summary>
        /// The on data loss async.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.OnDataLossAsync()
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "OnDataLossAsync()");
            return Task.FromResult(0);
        }

        /// <summary>
        /// The prepare checkpoint async.
        /// </summary>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.PrepareCheckpointAsync(CancellationToken cancellationToken)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "PrepareCheckpointAsync()");
            return Task.FromResult(0);
        }

        /// <summary>
        /// The perform checkpoint async.
        /// </summary>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.PerformCheckpointAsync(CancellationToken cancellationToken)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "PerformCheckpointAsync()");
            return Task.FromResult(0);
        }

        /// <summary>
        /// The complete checkpoint async.
        /// </summary>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.CompleteCheckpointAsync(CancellationToken cancellationToken)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "CompleteCheckpointAsync()");
            return Task.FromResult(0);
        }

        /// <summary>
        /// The recover checkpoint async.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.RecoverCheckpointAsync()
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "RecoverCheckpointAsync()");
            return Task.FromResult(0);
        }

        /// <summary>
        /// The on recovery completed async.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.OnRecoveryCompletedAsync()
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "OnRecoveryCompletedAsync()");
            return Task.FromResult(0);
        }

        /// <summary>
        /// The backup checkpoint async.
        /// </summary>
        /// <param name="backupDirectory">
        /// The backup directory.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.BackupCheckpointAsync(string backupDirectory, CancellationToken cancellationToken)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "BackupCheckpointAsync({0})", backupDirectory);
            return Task.FromResult(0);
        }

        /// <summary>
        /// The restore checkpoint async.
        /// </summary>
        /// <param name="backupDirectory">
        /// The backup directory.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.RestoreCheckpointAsync(string backupDirectory, CancellationToken cancellationToken)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "RestoreCheckpointAsync({0})", backupDirectory);
            return Task.FromResult(0);
        }

        /// <summary>
        /// The get current state.
        /// </summary>
        /// <returns>
        /// The <see cref="IOperationDataStream"/>.
        /// </returns>
        IOperationDataStream IStateProvider2.GetCurrentState()
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "GetCurrentState()");
            return new CopyStream(this.tables);
        }

        /// <summary>
        /// The begin setting current state.
        /// </summary>
        void IStateProvider2.BeginSettingCurrentState()
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "BeginSettingCurrentState()");
        }

        /// <summary>
        /// The set current state.
        /// </summary>
        /// <param name="stateRecordNumber">
        /// The state record number.
        /// </param>
        /// <param name="data">
        /// The data.
        /// </param>
        void IStateProvider2.SetCurrentState(long stateRecordNumber, OperationData data)
        {
            var count = data?.Count ?? 0;
            var length = data?.Sum(_ => _.Count) ?? 0;
            Trace.TraceInformation(
                "[" + this.partitionId + "] " + "SetCurrentState({0}, [{1} operations, {2}b])", 
                stateRecordNumber, 
                count, 
                length);

            /*
             * indexex: LSN, row key + partition key
             */
        }

        /// <summary>
        /// The end setting current state.
        /// </summary>
        void IStateProvider2.EndSettingCurrentState()
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "EndSettingCurrentState()");
        }

        /// <summary>
        /// The prepare for remove async.
        /// </summary>
        /// <param name="transaction">
        /// The transaction.
        /// </param>
        /// <param name="timeout">
        /// The timeout.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.PrepareForRemoveAsync(Transaction transaction, TimeSpan timeout, CancellationToken cancellationToken)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "PrepareForRemoveAsync()");
            return Task.FromResult(0);
        }

        /// <summary>
        /// The remove state async.
        /// </summary>
        /// <param name="stateProviderId">
        /// The state provider id.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.RemoveStateAsync(Guid stateProviderId)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "RemoveStateAsync({0})", stateProviderId);
            return Task.FromResult(0);
        }

        /// <summary>
        /// The apply async.
        /// </summary>
        /// <param name="lsn">
        /// The lsn.
        /// </param>
        /// <param name="transactionBase">
        /// The transaction base.
        /// </param>
        /// <param name="data">
        /// The data.
        /// </param>
        /// <param name="applyContext">
        /// The apply context.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task<object> IStateProvider2.ApplyAsync(
            long lsn,
            TransactionBase transactionBase,
            byte[] data,
            ApplyContext applyContext)
        {
            var context = default(OperationContext);
            var operation = Operation.Deserialize(data);
            if (IsPrimaryOperation(applyContext))
            {
                this.inProgressOperations.TryGetValue(operation.Id, out context);
            }

            var part = this.replicator.StatefulPartition;
            var operationString = JsonConvert.SerializeObject(operation, SerializationSettings.JsonConfig);
            Trace.TraceInformation(
                $"[{this.partitionId} r:{part.ReadStatus} w:{part.WriteStatus}] ApplyAsync(lsn: {lsn}, tx: {transactionBase.Id}, op: {operationString} (length: {data?.Length ?? 0}), context: {applyContext})");

            var table = context.Table ?? this.tables.Take();
            var transaction = context.DatabaseTransaction ?? new ESENT.Transaction(table.Session);

            try
            {
                ESENT.Api.JetSetSessionContext(table.Session, table.Context);
                var result = default(object);

                // If an existing transaction did not exist, apply the operation in the current transaction.
                if (context.DatabaseTransaction == null)
                {
                    Trace.TraceInformation($"{applyContext} Apply {operationString}");
                    result = operation.Apply(table);
                }

                Trace.TraceInformation($"{applyContext} Commit {operationString}");
                
                transaction.Commit(ESENT.CommitTransactionGrbit.None);

                if (applyContext == ApplyContext.PrimaryRedo && context.SuccessCallback != null)
                {
                    context.SuccessCallback(result);
                    context.SuccessCallback = null;
                    context.FailureCallback = null;
                }
            }
            catch (Exception exception) when (context.FailureCallback != null)
            {
                transaction.Rollback();

                context.FailureCallback(exception);
                context.SuccessCallback = null;
                context.FailureCallback = null;
                throw;
            }
            finally
            {
                ESENT.Api.JetResetSessionContext(table.Session);

                if (IsPrimaryOperation(applyContext))
                {
                    this.inProgressOperations.TryRemove(operation.Id, out context);
                }

                this.tables.Return(table);
            }

            return Task.FromResult(default(object));
        }

        private T PerformOperation<T>(long id, Transaction transaction, Operation undo, Operation redo)
        {
            T result;
            var table = this.tables.Take();
            var dbTransaction = default(ESENT.Transaction);
            var addedToInFlightOperations = false;
            try
            {
                ESENT.Api.JetSetSessionContext(table.Session, table.Context);
                dbTransaction = new ESENT.Transaction(table.Session);

                var part = this.replicator.StatefulPartition;
                Trace.TraceInformation(
                    $"[{this.partitionId} r:{part.ReadStatus} w:{part.WriteStatus}] PerformOperation(id: {id}, tx: {transaction.Id}, undo: {JsonConvert.SerializeObject(undo, SerializationSettings.JsonConfig)}, redo: {JsonConvert.SerializeObject(redo, SerializationSettings.JsonConfig)}");

                // Apply initially on primary, but do not commit.
                result = (T)redo.Apply(table);

                // Add the operation to the in-progress collection so that we can retrieve it when it is committed.
                addedToInFlightOperations = this.inProgressOperations.TryAdd(
                    id,
                    new OperationContext
                    {
                        ReplicatorTransaction = transaction,
                        DatabaseTransaction = dbTransaction,
                        Table = table
                    });

                if (!addedToInFlightOperations)
                {
                    throw new InvalidOperationException($"Operation with id {id} already in-progress.");
                }

                // Add this operation to the transaction.
                transaction.AddOperation(undo.Serialize(), redo.Serialize(), null, this.Name);
            }
            catch
            {
                dbTransaction?.Rollback();
                this.tables.Return(table);
                if (addedToInFlightOperations)
                {
                    OperationContext context;
                    this.inProgressOperations.TryRemove(id, out context);
                }

                throw;
            }
            finally
            {
                ESENT.Api.JetResetSessionContext(table.Session);
            }

            return result;
        }

        private static bool IsPrimaryOperation(ApplyContext applyContext)
        {
            return ((int)applyContext & (int)ApplyContext.PRIMARY) == (int)ApplyContext.PRIMARY;
        }

        /// <summary>
        /// The unlock.
        /// </summary>
        /// <param name="state">
        /// The state.
        /// </param>
        void IStateProvider2.Unlock(object state)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "Unlock()");
        }

        /// <summary>
        /// Returns the child state providers.
        /// </summary>
        /// <param name="name">
        /// The state provider name.
        /// </param>
        /// <returns>
        /// The child state providers.
        /// </returns>
        IEnumerable<IStateProvider2> IStateProvider2.GetChildren(Uri name)
        {
            Trace.TraceInformation("[" + this.partitionId + "] " + "GetChildren()");
            return Enumerable.Empty<IStateProvider2>();
        }

        /// <summary>
        /// Provides the stream of operations required to copy this store.
        /// </summary>
        internal class CopyStream : IOperationDataStream
        {
            /// <summary>
            /// The values.
            /// </summary>
            private readonly IEnumerator<KeyValuePair<TKey, TValue>> values;

            /// <summary>
            /// The pool of tables.
            /// </summary>
            private readonly PersistentTablePool<TKey, TValue> pool;

            /// <summary>
            /// The table.
            /// </summary>
            private PersistentTable<TKey, TValue> table;

            /// <summary>
            /// Initializes a new instance of the <see cref="CopyStream"/> class.
            /// </summary>
            /// <param name="pool">
            /// The table pool.
            /// </param>
            public CopyStream(PersistentTablePool<TKey, TValue> pool)
            {
                this.pool = pool;
                this.table = pool.Take();
                this.values = this.table.GetRange().GetEnumerator();
            }

            /// <summary>
            /// The get next async.
            /// </summary>
            /// <param name="cancellationToken">
            /// The cancellation token.
            /// </param>
            /// <returns>
            /// The <see cref="Task"/>.
            /// </returns>
            public Task<OperationData> GetNextAsync(CancellationToken cancellationToken)
            {
                if (this.values.MoveNext())
                {
                    var element = this.values.Current;
                    var data =
                        new OperationData(new SetOperation { Key = element.Key, Value = element.Value }.Serialize());
                    return Task.FromResult(data);
                }

                if (this.table == null)
                {
                    return Task.FromResult(default(OperationData));
                }

                this.pool.Return(this.table);
                this.table = null;

                return Task.FromResult(default(OperationData));
            }
        }

        /// <summary>
        /// Represents the context of an operation.
        /// </summary>
        internal struct OperationContext
        {
            /// <summary>
            /// Gets or sets the Service Fabric replicator transaction.
            /// </summary>
            public Transaction ReplicatorTransaction { get; set; }

            /// <summary>
            /// Gets or sets the database transaction.
            /// </summary>
            public ESENT.Transaction DatabaseTransaction { get; set; }

            /// <summary>
            /// Gets or sets the table.
            /// </summary>
            public PersistentTable<TKey, TValue> Table { get; set; }

            /// <summary>
            /// The operation to execute on successful completion.
            /// </summary>
            public Action<object> SuccessCallback { get; set; }

            /// <summary>
            /// The operation to execute on failure.
            /// </summary>
            public Action<Exception> FailureCallback { get; set; }
        }
    }
}
