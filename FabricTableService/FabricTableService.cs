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
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;

    using global::FabricTableService.Journal;

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

            var partition = this.ServicePartition.PartitionInfo.Id;
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

                    ServiceEventSource.Current.ServiceMessage(this, "Initial value: " + await journal.GetValue(Guid.Empty));
                    await journal.SetValue(tx, Guid.Empty, DateTime.Now.ToString(CultureInfo.InvariantCulture));
                    ServiceEventSource.Current.ServiceMessage(this, "Final value: " + await journal.GetValue(Guid.Empty));

                    await tx.CommitAsync();
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                if (--x <= 0) break;
            }
        }
    }
}