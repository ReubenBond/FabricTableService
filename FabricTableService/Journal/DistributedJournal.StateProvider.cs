namespace FabricTableService.Journal
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Fabric;
    using System.Fabric.Replication;
    using System.IO;
    using System.Linq;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.ServiceFabric.Data;

    using Transaction = System.Fabric.Replication.Transaction;
    using TransactionBase = System.Fabric.Replication.TransactionBase;

    public partial class DistributedJournal : IStateProvider2, IReliableState
    {
        /// <summary>
        /// The state replicator.
        /// </summary>
        private TransactionalReplicator replicator;

        /// <summary>
        /// Gets the name of this provider.
        /// </summary>
        public Uri Name { get; private set; }

        /// <summary>
        /// Gets the initialization context.
        /// </summary>
        public byte[] InitializationContext { get; private set; }
        
        void IStateProvider2.Initialize(
            TransactionalReplicator transactionalReplicator,
            Uri name,
            byte[] initializationContext,
            Guid stateProviderId)
        {
            Debug.WriteLine(
                "Initialize({0}, {1}, {2})",
                name,
                initializationContext == null ? string.Empty : string.Concat(initializationContext.Select(_ => string.Format("{0:X}", _))),
                stateProviderId);
            this.replicator = transactionalReplicator;
            this.InitializationContext = initializationContext;
            this.Name = name;
        }

        Task IStateProvider2.OpenAsync()
        {
            Debug.WriteLine("OpenAsync()");
            var tableName = Regex.Replace(this.Name.ToString(), @"[^a-zA-Z0-9_\-]", string.Empty);
            var workDirectory = this.replicator.InitializationParameters.CodePackageActivationContext.WorkDirectory;
            var databaseDirectory = Path.Combine(
                workDirectory, this.replicator.StatefulPartition.PartitionInfo.Id.ToString("N"));
            Directory.CreateDirectory(databaseDirectory);
            this.table = new PersistentTable(databaseDirectory, "journal.edb", tableName);

            this.table.Initialize();
            return Task.FromResult(0);
        }

        Task IStateProvider2.CloseAsync()
        {
            Debug.WriteLine("CloseAsync()");
            var tab = this.table;
            if (tab != null)
            {
                tab.Dispose();
                this.table = null;
            }

            return Task.FromResult(0);
        }

        void IStateProvider2.Abort()
        {
        }

        Task IStateProvider2.ChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            Debug.WriteLine("ChangeRoleAsync({0})", newRole);
            return Task.FromResult(0);
        }

        Task IStateProvider2.OnDataLossAsync()
        {
            Debug.WriteLine("OnDataLossAsync()");
            return Task.FromResult(0);
        }

        Task IStateProvider2.PrepareCheckpointAsync(CancellationToken cancellationToken)
        {
            Debug.WriteLine("PrepareCheckpointAsync()");
            return Task.FromResult(0);
        }

        Task IStateProvider2.PerformCheckpointAsync(CancellationToken cancellationToken)
        {
            Debug.WriteLine("PerformCheckpointAsync()");
            return Task.FromResult(0);
        }

        Task IStateProvider2.CompleteCheckpointAsync(CancellationToken cancellationToken)
        {
            Debug.WriteLine("CompleteCheckpointAsync()");
            return Task.FromResult(0);
        }

        Task IStateProvider2.RecoverCheckpointAsync()
        {
            Debug.WriteLine("RecoverCheckpointAsync()");
            return Task.FromResult(0);
        }

        Task IStateProvider2.OnRecoveryCompletedAsync()
        {
            Debug.WriteLine("OnRecoveryCompletedAsync()");
            return Task.FromResult(0);
        }

        Task IStateProvider2.BackupCheckpointAsync(string backupDirectory, CancellationToken cancellationToken)
        {
            Debug.WriteLine("BackupCheckpointAsync({0})", backupDirectory);
            return Task.FromResult(0);
        }

        Task IStateProvider2.RestoreCheckpointAsync(string backupDirectory, CancellationToken cancellationToken)
        {
            Debug.WriteLine("RestoreCheckpointAsync({0})", backupDirectory);
            return Task.FromResult(0);
        }

        IOperationDataStream IStateProvider2.GetCurrentState()
        {
            Debug.WriteLine("GetCurrentState()");
            return new OperationDataStream(this.table);
        }

        void IStateProvider2.BeginSettingCurrentState()
        {
            Debug.WriteLine("BeginSettingCurrentState()");
        }

        void IStateProvider2.SetCurrentState(long stateRecordNumber, OperationData data)
        {
            var count = data == null ? 0 : data.Count;
            var length = data == null ? 0 : data.Sum(_ => _.Count);
            Debug.WriteLine("SetCurrentState({0}, [{1} operations, {2}b])", stateRecordNumber, count, length);
            /*
             * indexex: LSN, row key + partition key
             */
        }

        void IStateProvider2.EndSettingCurrentState()
        {
            Debug.WriteLine("EndSettingCurrentState()");
        }

        Task IStateProvider2.PrepareForRemoveAsync(Transaction transaction, TimeSpan timeout, CancellationToken cancellationToken)
        {
            Debug.WriteLine("PrepareForRemoveAsync()");
            return Task.FromResult(0);
        }

        Task IStateProvider2.RemoveStateAsync(Guid stateProviderId)
        {
            Debug.WriteLine("RemoveStateAsync({0})", stateProviderId);
            return Task.FromResult(0);
        }

        Task<object> IStateProvider2.ApplyAsync(long lsn, TransactionBase transactionBase, byte[] data, ApplyContext applyContext)
        {
            Debug.WriteLine(
                "ApplyAsync(lsn: {0}, tx: {1}, length {2}, context: {3})",
                lsn,
                transactionBase.Id,
                data == null ? 0 : data.Length,
                applyContext);

            var operation = Operation.Deserialize(data);
            operation.Apply(this);

            return Task.FromResult(default(object));
        }

        void IStateProvider2.Unlock(object state)
        {
            Debug.WriteLine("Unlock()");
        }

        IEnumerable<IStateProvider2> IStateProvider2.GetChildren(Uri name)
        {
            Debug.WriteLine("GetChildren()");
            return Enumerable.Empty<IStateProvider2>();
        }
    }

    internal class EmptyOperationDataStream : IOperationDataStream
    {
        public Task<OperationData> GetNextAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(new OperationData());
        }
    }

    internal class OperationDataStream : IOperationDataStream
    {
        private readonly IEnumerator<KeyValuePair<Guid, string>> values;
        
        public OperationDataStream(PersistentTable table)
        {
            this.values = table.GetRange().GetEnumerator();
        }

        public Task<OperationData> GetNextAsync(CancellationToken cancellationToken)
        {
            if (this.values.MoveNext())
            {
                var data = new OperationData(new DistributedJournal.SetOperation { Value = this.values.Current.Value }.Serialize());
                return Task.FromResult(data);
            }

            return Task.FromResult(new OperationData());
        }
    }
}
