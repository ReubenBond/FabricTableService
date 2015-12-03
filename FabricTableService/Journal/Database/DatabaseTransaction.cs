namespace FabricTableService.Journal.Database
{
    using System;

    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// Represents a database transaction.
    /// </summary>
    /// <typeparam name="TKey">The database key type.</typeparam>
    /// <typeparam name="TValue">The database value type.</typeparam>
    public struct DatabaseTransaction<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// Gets or sets pool of tables which this instance came from.
        /// </summary>
        public PersistentTablePool<TKey, TValue> Pool { get; set; }

        /// <summary>
        /// Gets or sets the table referenced in this transaction.
        /// </summary>
        public PersistentTable<TKey, TValue> Table { get; set; }

        /// <summary>
        /// Gets or sets the underlying transaction object.
        /// </summary>
        public Transaction Transaction { get; private set; }
        
        /// <summary>
        /// Starts this transaction.
        /// </summary>
        public void Start()
        {
            Api.JetSetSessionContext(this.Table.Session, this.Table.Context);
            this.Transaction = new Transaction(this.Table.Session);
        }

        /// <summary>
        /// Pauses this transaction so that it can move to another thread.
        /// </summary>
        public void Pause()
        {
            Api.JetResetSessionContext(this.Table.Session);
        }

        /// <summary>
        /// Resumes this transaction on the current thread.
        /// </summary>
        public void Resume()
        {
            Api.JetSetSessionContext(this.Table.Session, this.Table.Context);
        }

        /// <summary>
        /// Rolls this transaction back, undoing all changes.
        /// </summary>
        public void Rollback()
        {
            this.Transaction.Rollback();
        }

        /// <summary>
        /// Commits this transaction.
        /// </summary>
        public void Commit()
        {
            this.Transaction.Commit(CommitTransactionGrbit.None);
        }

        /// <summary>
        /// Disposes this transaction.
        /// </summary>
        public void Dispose()
        {
            this.Transaction?.Dispose();
            if (this.Pool == null)
            {
                return;
            }

            Api.JetResetSessionContext(this.Table.Session);
            this.Pool.Return(this.Table);

            this.Table = null;
            this.Transaction = null;
            this.Pool = null;
        }
    }
}