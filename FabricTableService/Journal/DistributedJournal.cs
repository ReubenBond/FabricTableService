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
    using System.Fabric.Replication;
    using System.Threading.Tasks;

    /// <summary>
    /// The distributed journal.
    /// </summary>
    public partial class DistributedJournal
    {
        public Task SetValue(Transaction tx, string value)
        {
            tx.AddOperation()
        }
    }
}
