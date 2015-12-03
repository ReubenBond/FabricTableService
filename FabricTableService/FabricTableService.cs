using System.Threading;
using System.Threading.Tasks;
using FabricTableService.Journal;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services;
using Microsoft.ServiceFabric.Services.Wcf;

namespace FabricTableService
{
    using global::FabricTableService.Interface;

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
        /// Creates and returns a communication listener.
        /// </summary>
        /// <returns>
        /// A new <see cref="ICommunicationListener"/>.
        /// </returns>
        protected override ICommunicationListener CreateCommunicationListener()
        {
            return new WcfCommunicationListener(typeof(ITableStoreService), this)
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
            var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, byte[]>>("myDictionary");
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