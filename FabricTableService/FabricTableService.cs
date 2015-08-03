using System.Threading;
using System.Threading.Tasks;
using FabricTableService.Journal;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services;
using Microsoft.ServiceFabric.Services.Wcf;
using TableStore.Interface;

namespace FabricTableService
{
    /// <summary>
    /// The fabric table service.
    /// </summary>
    public class FabricTableService : StatefulService, ITableStoreService
    {
        private readonly TaskCompletionSource<DistributedJournal<string, byte[]>> journalTask = new TaskCompletionSource<DistributedJournal<string, byte[]>>();

        public async Task Delete(string key, string partition)
        {
            var journal = await journalTask.Task;
            using (var tx = this.StateManager.CreateTransaction())
            {
                journal.TryRemove(tx, key);
                await tx.CommitAsync();
            }
        }

        public async Task<byte[]> Get(string key, string partition)
        {
            var journal = await journalTask.Task;
            using (var tx = this.StateManager.CreateTransaction())
            {
                var value = journal.GetValue(tx, key);
                await tx.CommitAsync();
                return value.Item2;
            }
        }

        public async Task Insert(string key, string partition, byte[] value)
        {
            var journal = await journalTask.Task;
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
            //var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");
            this.journalTask.SetResult(
                await this.StateManager.GetOrAddAsync<DistributedJournal<string, byte[]>>("journal"));
            
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(5000, cancellationToken);
            }

/*            var backupPath =
                Path.Combine(
                    this.ServiceInitializationParameters.CodePackageActivationContext.WorkDirectory,
                    "backup",
                    this.ServicePartition.PartitionInfo.Id.ToString("N"));
            await journal.Backup(backupPath);
            Trace.TraceInformation($"Backed up to {backupPath}. Counter value is {journal.GetValue(Guid.Empty)}");

            var journal2 = new PersistentTablePool<Guid, string>(backupPath, "db.edb", "journal");
            await journal2.Restore(backupPath, @"c:\tmp\db");
            Trace.TraceInformation($"Restored from {backupPath}. Counter value is {journal.GetValue(Guid.Empty)}");
            

            journal.Dispose();*/
        }
    }
}