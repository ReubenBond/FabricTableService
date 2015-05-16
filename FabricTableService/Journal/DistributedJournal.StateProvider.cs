namespace FabricTableService.Journal
{
    using System;
    using System.Collections.Generic;
    using System.Fabric;
    using System.Fabric.Replication;
    using System.Linq;
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
            this.replicator = transactionalReplicator;
            this.InitializationContext = initializationContext;
            this.Name = name;
        }

        Task IStateProvider2.OpenAsync()
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.CloseAsync()
        {
            return Task.FromResult(0);
        }

        void IStateProvider2.Abort()
        {
        }

        Task IStateProvider2.ChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.OnDataLossAsync()
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.PrepareCheckpointAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.PerformCheckpointAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.CompleteCheckpointAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.RecoverCheckpointAsync()
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.OnRecoveryCompletedAsync()
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.BackupCheckpointAsync(string backupDirectory, CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.RestoreCheckpointAsync(string backupDirectory, CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        IOperationDataStream IStateProvider2.GetCurrentState()
        {
            return new EmptyOperationDataStream();
        }

        void IStateProvider2.BeginSettingCurrentState()
        {
        }

        void IStateProvider2.SetCurrentState(long stateRecordNumber, OperationData data)
        {
        }

        void IStateProvider2.EndSettingCurrentState()
        {
        }

        Task IStateProvider2.PrepareForRemoveAsync(Transaction transaction, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        Task IStateProvider2.RemoveStateAsync(Guid StateProviderId)
        {
            return Task.FromResult(0);
        }

        Task<object> IStateProvider2.ApplyAsync(long lsn, TransactionBase transactionBase, byte[] data, ApplyContext applyContext)
        {
            return Task.FromResult(default(object));
        }

        void IStateProvider2.Unlock(object state)
        {
        }

        IEnumerable<IStateProvider2> IStateProvider2.GetChildren(Uri name)
        {
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
}
