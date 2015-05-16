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
    using System;

    using Microsoft.Isam.Esent.Interop;
    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// The distributed journal.
    /// </summary>
    public partial class DistributedJournal
    {
        /// <summary>
        /// The table.
        /// </summary>
        private PersistentTable table;

        /// <summary>
        /// The set value.
        /// </summary>
        /// <param name="tx">
        /// The tx.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        public void SetValue(ITransaction tx, string value)
        {
            var transaction = tx.GetTransaction();
            var initialValue = this.GetValue();
            transaction.AddOperation(
                new SetOperation { Value = initialValue }.Serialize(), 
                new SetOperation { Value = value }.Serialize(), 
                null, 
                this.Name);
        }

        /// <summary>
        /// The get value.
        /// </summary>
        /// <returns>
        /// The <see cref="string"/>.
        /// </returns>
        public string GetValue()
        {
            using (var tx = new Microsoft.Isam.Esent.Interop.Transaction(this.table.Session))
            {
                string result;
                this.table.TryGetValue(Guid.Empty, out result);
                tx.Commit(CommitTransactionGrbit.None);
                return result;
            }
        }
    }
}
