// --------------------------------------------------------------------------------------------------------------------
// <copyright file="FabricTableService.cs" company="">
//   
// </copyright>
// <summary>
//   The fabric table service.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace FabricTableService
{
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    using global::FabricTableService.Journal;
    using global::FabricTableService.Journal.Database;

    using Microsoft.ServiceFabric.Data.Collections;
    using Microsoft.ServiceFabric.Services;

    /// <summary>
    /// The fabric table service.
    /// </summary>
    public class FabricTableService : StatefulService
    {
        /// <summary>
        /// The create communication listener.
        /// </summary>
        /// <returns>
        /// The <see cref="ICommunicationListener"/>.
        /// </returns>
        protected override ICommunicationListener CreateCommunicationListener()
        {
            // TODO: Replace this with an ICommunicationListener implementation if your service needs to handle user requests.
            return base.CreateCommunicationListener();
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
            // TODO: Replace the following with your own logic.
            var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");
            var journal = await this.StateManager.GetOrAddAsync<DistributedJournal<Guid, string>>("journal");
            
            int x = 100;
            while (!cancellationToken.IsCancellationRequested)
            {
                using (var tx = this.StateManager.CreateTransaction())
                {
                    var result = await myDictionary.TryGetValueAsync(tx, "Counter-1");
                    ServiceEventSource.Current.ServiceMessage(
                        this, 
                        "Current Counter Value: {0}", 
                        result.HasValue ? result.Value.ToString() : "Value does not exist.");

                    await myDictionary.AddOrUpdateAsync(tx, "Counter-1", 0, (k, v) => ++v);

                    var value = journal.GetValue(Guid.Empty);
                    ServiceEventSource.Current.ServiceMessage(this, "Initial value: " + value);
                    int intVal;
                    int.TryParse(value, out intVal);
                    journal.SetValue(tx, Guid.Empty, (intVal + 1).ToString());
                    ServiceEventSource.Current.ServiceMessage(this, "Final value: " + journal.GetValue(Guid.Empty));

                    await tx.CommitAsync();
                }
                
                if (--x <= 0) break;
            }

            var backupPath =
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
            ((IDisposable)journal2).Dispose();
            
            journal.Dispose();
        }
    }
}