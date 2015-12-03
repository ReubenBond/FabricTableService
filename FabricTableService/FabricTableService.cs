using System.Threading;
using System.Threading.Tasks;
using FabricTableService.Journal;

namespace FabricTableService
{
    using System.Collections.Generic;
    using System.Fabric;

    using global::FabricTableService.Interface;

    using Microsoft.ServiceFabric.Services.Communication.Runtime;
    using Microsoft.ServiceFabric.Services.Communication.Wcf.Runtime;
    using Microsoft.ServiceFabric.Services.Runtime;

    /// <summary>
    /// The fabric table service.
    /// </summary>
    public class FabricTableService : StatefulService, ITableStoreService
    {
        private readonly TaskCompletionSource<ReliableTable<string, byte[]>> journalTask = new TaskCompletionSource<ReliableTable<string, byte[]>>();

        public async Task Delete(string key, string partition)
        {
            var journal = await this.journalTask.Task;
            using (var tx = this.StateManager.CreateTransaction())
            {
                journal.TryRemove(tx, key);
                await tx.CommitAsync();
            }
        }

        public async Task<byte[]> Get(string key, string partition)
        {
            var journal = await this.journalTask.Task;
            using (var tx = this.StateManager.CreateTransaction())
            {
                var result = journal.GetValue(tx, key);
                await tx.CommitAsync();
                return result.Value;
            }
        }

        public async Task Insert(string key, string partition, byte[] value)
        {
            var journal = await this.journalTask.Task;
            using (var tx = this.StateManager.CreateTransaction())
            {
                journal.SetValue(tx, key, value);
                await tx.CommitAsync();
            }
        }

        /// <summary>
        /// Override this method to supply the communication listeners for the service replica. The endpoints returned by the communication listener's
        ///             are stored as a JSON string of ListenerName, Endpoint string pairs like 
        ///             {"Endpoints":{"Listener1":"Endpoint1","Listener2":"Endpoint2" ...}}
        /// </summary>
        /// <returns>
        /// List of ServiceReplicaListeners
        /// </returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[] { new ServiceReplicaListener(this.CreateCommunicationListener), };
        }

        /// <summary>
        /// Creates and returns a communication listener.
        /// </summary>
        /// <param name="initializationParameters">
        /// The service initialization parameters.
        /// </param>
        /// <returns>
        /// A new <see cref="ICommunicationListener"/>.
        /// </returns>
        private ICommunicationListener CreateCommunicationListener(StatefulServiceInitializationParameters initializationParameters)
        {
            return new WcfCommunicationListener(initializationParameters, typeof(ITableStoreService), this)
            {
                Binding = ServiceBindings.TcpBinding,
                EndpointResourceName = "ServiceEndpoint"
            };
        }

        /// <summary>
        /// The run async.
        /// </summary>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            var journal = await this.StateManager.GetOrAddAsync<ReliableTable<string, byte[]>>("journal");
            this.journalTask.SetResult(journal);
            

            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(5000, cancellationToken);
            }

/*            var backupPath =
                Path.Combine(
                    this.ServiceInitializationParameters.CodePackageActivationContext.WorkDirectory,
                    "backup",
                    this.ServicePartition.PartitionInfo.Id.ToString("N"));
            Trace.TraceInformation($"Backing up to {backupPath}. Counter value is {journal.GetValue(Guid.Empty)}");
            await journal.Backup(backupPath);
            Trace.TraceInformation($"Backed up to {backupPath}. Counter value is {journal.GetValue(Guid.Empty)}");

            Trace.TraceInformation($"Restoring from {backupPath}");
            var journal2 = new PersistentTablePool<Guid, string>(backupPath, "db.edb", "journal");
            await journal2.Restore(backupPath, @"c:\tmp\db");
            var journal2Client = journal2.Take();
            Trace.TraceInformation($"Restored from {backupPath}. Counter value is {journal2Client.Get(Guid.Empty)}");
            journal.Dispose();*/
        }
    }
}