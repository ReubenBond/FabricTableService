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

    using Microsoft.ServiceFabric.Data;

    using Newtonsoft.Json;

    using Transaction = System.Fabric.Replication.Transaction;
    using TransactionBase = System.Fabric.Replication.TransactionBase;

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
                                    : string.Concat(initializationContext.Select(_ => string.Format("{0:X}", _)));
            Debug.WriteLine(
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
            Debug.WriteLine("[" + this.partitionId + "] " + "OpenAsync()");

            var tableName = Regex.Replace(this.Name.ToString(), @"[^a-zA-Z0-9_\-]", string.Empty);
            var workDirectory = this.replicator.InitializationParameters.CodePackageActivationContext.WorkDirectory;
            var databaseDirectory = Path.Combine(workDirectory, this.partitionId, "journal");
            Directory.CreateDirectory(databaseDirectory);
            this.table = new PersistentTable<TKey, TValue>(databaseDirectory, "db.edb", tableName);
            this.table.Initialize();
            this.operationPump.Start();
            return Task.FromResult(0);
        }

        /// <summary>
        /// The close async.
        /// </summary>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        Task IStateProvider2.CloseAsync()
        {
            Debug.WriteLine("[" + this.partitionId + "] " + "CloseAsync()");
            var tab = this.table;
            if (tab != null)
            {
                tab.Dispose();
                this.table = null;
            }

            return Task.FromResult(0);
        }

        /// <summary>
        /// The abort.
        /// </summary>
        void IStateProvider2.Abort()
        {
            Debug.WriteLine("[" + this.partitionId + "] " + "Abort()");
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
            Debug.WriteLine("[" + this.partitionId + "] " + "ChangeRoleAsync({0})", newRole);
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
            Debug.WriteLine("[" + this.partitionId + "] " + "OnDataLossAsync()");
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
            Debug.WriteLine("[" + this.partitionId + "] " + "PrepareCheckpointAsync()");
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
            Debug.WriteLine("[" + this.partitionId + "] " + "PerformCheckpointAsync()");
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
            Debug.WriteLine("[" + this.partitionId + "] " + "CompleteCheckpointAsync()");
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
            Debug.WriteLine("[" + this.partitionId + "] " + "RecoverCheckpointAsync()");
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
            Debug.WriteLine("[" + this.partitionId + "] " + "OnRecoveryCompletedAsync()");
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
            Debug.WriteLine("[" + this.partitionId + "] " + "BackupCheckpointAsync({0})", backupDirectory);
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
            Debug.WriteLine("[" + this.partitionId + "] " + "RestoreCheckpointAsync({0})", backupDirectory);
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
            Debug.WriteLine("[" + this.partitionId + "] " + "GetCurrentState()");
            return new OperationDataStream(this.table);
        }

        /// <summary>
        /// The begin setting current state.
        /// </summary>
        void IStateProvider2.BeginSettingCurrentState()
        {
            Debug.WriteLine("[" + this.partitionId + "] " + "BeginSettingCurrentState()");
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
            var count = data == null ? 0 : data.Count;
            var length = data == null ? 0 : data.Sum(_ => _.Count);
            Debug.WriteLine(
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
            Debug.WriteLine("[" + this.partitionId + "] " + "EndSettingCurrentState()");
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
            Debug.WriteLine("[" + this.partitionId + "] " + "PrepareForRemoveAsync()");
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
            Debug.WriteLine("[" + this.partitionId + "] " + "RemoveStateAsync({0})", stateProviderId);
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
            return this.operationPump.Invoke(
                () =>
                {
                    var operation = Operation.Deserialize(data);

                    Debug.WriteLine(
                        "[" + this.partitionId + "] " + "ApplyAsync(lsn: {0}, tx: {1}, op: {2} (length: {3}), context: {4})", 
                        lsn, 
                        transactionBase.Id, 
                        JsonConvert.SerializeObject(operation), 
                            data == null ? 0 : data.Length, 
                            applyContext);
                    operation.Apply(this);
                    return default(object);
                });
        }

        /// <summary>
        /// The unlock.
        /// </summary>
        /// <param name="state">
        /// The state.
        /// </param>
        void IStateProvider2.Unlock(object state)
        {
            Debug.WriteLine("[" + this.partitionId + "] " + "Unlock()");
        }

        /// <summary>
        /// The get children.
        /// </summary>
        /// <param name="name">
        /// The name.
        /// </param>
        /// <returns>
        /// The <see cref="IEnumerable"/>.
        /// </returns>
        IEnumerable<IStateProvider2> IStateProvider2.GetChildren(Uri name)
        {
            Debug.WriteLine("[" + this.partitionId + "] " + "GetChildren()");
            return Enumerable.Empty<IStateProvider2>();
        }

        /// <summary>
        /// The operation data stream.
        /// </summary>
        internal class OperationDataStream : IOperationDataStream
        {
            /// <summary>
            /// The values.
            /// </summary>
            private readonly IEnumerator<KeyValuePair<TKey, TValue>> values;

            /// <summary>
            /// Initializes a new instance of the <see cref="OperationDataStream"/> class.
            /// </summary>
            /// <param name="table">
            /// The table.
            /// </param>
            public OperationDataStream(PersistentTable<TKey, TValue> table)
            {
                this.values = table.GetRange().GetEnumerator();
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
                        new OperationData(
                            new SetOperation { Key = element.Key, Value = element.Value }
                                .Serialize());
                    return Task.FromResult(data);
                }

                return Task.FromResult(new OperationData());
            }
        }
    }
}
