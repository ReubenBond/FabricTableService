namespace FabricTableService.Journal.Database
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.InteropServices;

    using ESENT = Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// A persistent table.
    /// </summary>
    public class PersistentTable<TKey, TValue> : IDisposable
    {
        internal PersistentTable()
        {
        }

        /// <summary>
        /// The key column.
        /// </summary>
        internal ESENT.JET_COLUMNID KeyColumn { get; set; }

        /// <summary>
        /// The value column.
        /// </summary>
        internal ESENT.JET_COLUMNID ValueColumn { get; set; }

        /// <summary>
        /// The table.
        /// </summary>
        internal ESENT.Table Table { get; set; }

        /// <summary>
        /// The session.
        /// </summary>
        internal ESENT.Session Session { get; set; }

        /// <summary>
        /// The session context handle, which exists to support multi-threaded access to the session.
        /// </summary>
        internal GCHandle SessionHandle { get; set; }

        /// <summary>
        /// The session context.
        /// </summary>
        internal IntPtr Context => (IntPtr)this.SessionHandle;

        public bool TryGetValue(TKey key, out TValue value)
        {
            value = this.GetInternal(key);
            return value != null;
        }

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addFunc, Func<TKey, TValue, TValue> updateFunc)
        {
            var existing = this.GetInternal(key);
            TValue newValue;
            if (existing != null)
            {
                newValue = updateFunc(key, existing);
                this.Update(key, newValue);
            }
            else
            {
                newValue = addFunc(key);
                this.Add(key, newValue);
            }

            return newValue;
        }

        /// <summary>
        /// Inserts the key-value pair if the key is not present in the table, or replaces if it is.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The updated value.</param>
        public void AddOrUpdate(TKey key, TValue value)
        {
            if (this.Contains(key))
            {
                this.Update(key, value);
            }
            else
            {
                this.Add(key, value);
            }
        }

        /// <summary>
        /// Adds or updates the specified value, or adds it and returns the result.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="updateFunc">The function used to create the value to update with if one does not already exist.</param>
        /// <returns>The added or existing value.</returns>
        public TValue AddOrUpdate(TKey key, TValue value, Func<TKey, TValue, TValue> updateFunc)
        {
            var existing = this.GetInternal(key);
            if (existing != null)
            {
                value = updateFunc(key, existing);
                this.Update(key, value);
            }
            else
            {
                this.Add(key, value);
            }

            return value;
        }

        /// <summary>
        /// Retrieves the specified value, or adds it and returns the result.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="addFunc">The function used to create the value to add if one does not already exist.</param>
        /// <returns>The added or existing value.</returns>
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> addFunc)
        {
            var existing = this.GetInternal(key);
            if (existing != null)
            {
                return existing;
            }

            var value = addFunc(key);
            this.Add(key, value);
            return value;
        }

        /// <summary>
        /// Retrieves the specified value, or adds it and returns the result.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value to add if it does not already exist.</param>
        /// <returns>The added or existing value.</returns>
        public TValue GetOrAdd(TKey key, TValue value)
        {
            var existing = this.GetInternal(key);
            if (existing != null)
            {
                return existing;
            }

            this.Add(key, value);
            return value;
        }

        /// <summary>
        /// Updates the specified entry.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public void Update(TKey key, TValue value)
        {
            this.UpdateRow(key, value, ESENT.JET_prep.Replace);
        }

        /// <summary>
        /// Adds a new value.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public void Add(TKey key, TValue value)
        {
            this.UpdateRow(key, value, ESENT.JET_prep.Insert);
        }

        /// <summary>
        /// Returns <see langword="true"/> if the key will be deleted when the transaction commits, <see langword="false"/> otherwise.
        /// </summary>
        /// <param name="key">
        /// The key to search for.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the key will be deleted when the transaction commits, <see langword="false"/> otherwise.
        /// </returns>
        public bool Contains(TKey key)
        {
            ESENT.Api.JetSetCurrentIndex(this.Session, this.Table, PersistentTableConstants.PrimaryIndexName);
            PersistentTablePool<TKey, TValue>.Converters.MakeKey(this.Session, this.Table, key, ESENT.MakeKeyGrbit.NewKey);
            
            if (!ESENT.Api.TrySeek(this.Session, this.Table, ESENT.SeekGrbit.SeekEQ))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Retries the value of the specified entry.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>The value.</returns>
        public TValue Get(TKey key)
        {
            TValue value;
            if (!this.TryGetValue(key, out value))
            {
                throw new KeyNotFoundException(key.ToString());
            }

            return value;
        }

        /// <summary>
        /// Deletes the row with the specified <paramref name="key"/>.
        /// </summary>
        /// <param name="key">
        /// The key to delete.
        /// </param>
        public void Remove(TKey key)
        {
            TValue value;
            if (!this.TryRemove(key, out value))
            {
                throw new KeyNotFoundException(key.ToString());
            }
        }

        /// <summary>
        /// Deletes the row with the specified <paramref name="key"/>.
        /// </summary>
        /// <param name="key">
        /// The key to delete.
        /// </param>
        /// <param name="value">
        /// The value which was removed.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the key will be deleted when the transaction commits, <see langword="false"/> otherwise.
        /// </returns>
        public bool TryRemove(TKey key, out TValue value)
        {
            ESENT.Api.JetSetCurrentIndex(this.Session, this.Table, PersistentTableConstants.PrimaryIndexName);
            PersistentTablePool<TKey, TValue>.Converters.MakeKey(
                this.Session,
                this.Table,
                key,
                ESENT.MakeKeyGrbit.NewKey);

            if (!ESENT.Api.TrySeek(this.Session, this.Table, ESENT.SeekGrbit.SeekEQ))
            {
                value = default(TValue);
                return false;
            }

            value =
                (TValue)
                PersistentTablePool<TKey, TValue>.Converters.RetrieveValueColumn(
                    this.Session,
                    this.Table,
                    this.ValueColumn);
            ESENT.Api.JetDelete(this.Session, this.Table);
            return true;
        }

        /// <summary>
        /// Returns a collection of rows matching the specified criteria.
        /// </summary>
        /// <param name="lowerBound">
        /// The lower bound, inclusive, or <see langword="null"/> to query from the beginning.
        /// </param>
        /// <param name="upperBound">
        /// The upper bound, inclusive, or <see langword="null"/> to query until the end or the maximum value count has been reached.
        /// </param>
        /// <param name="maxValues">
        /// The maximum number of rows to return.
        /// </param>
        /// <returns>
        /// The rows matching the specified criteria.
        /// </returns>
        public IEnumerable<KeyValuePair<TKey, TValue>> GetRange(
            TKey lowerBound = default(TKey),
            TKey upperBound = default(TKey),
            long maxValues = long.MaxValue)
        {
            // Set the index and seek to the lower bound, if it has been specified.
            ESENT.Api.JetSetCurrentIndex(this.Session, this.Table, PersistentTableConstants.PrimaryIndexName);
            PersistentTablePool<TKey, TValue>.Converters.MakeKey(
                this.Session,
                this.Table,
                lowerBound,
                ESENT.MakeKeyGrbit.NewKey);
            if (!ESENT.Api.TrySeek(this.Session, this.Table, ESENT.SeekGrbit.SeekGE))
            {
                yield break;
            }

            // Set the upper limit of the index scan.
            PersistentTablePool<TKey, TValue>.Converters.MakeKey(
                this.Session,
                this.Table,
                upperBound,
                ESENT.MakeKeyGrbit.NewKey | ESENT.MakeKeyGrbit.FullColumnEndLimit);
            const ESENT.SetIndexRangeGrbit RangeFlags =
                ESENT.SetIndexRangeGrbit.RangeInclusive | ESENT.SetIndexRangeGrbit.RangeUpperLimit;
            if (!ESENT.Api.TrySetIndexRange(this.Session, this.Table, RangeFlags))
            {
                yield break;
            }

            // Iterate over the ranged index.
            bool hasNext;
            do
            {
                var key =
                    (TKey)
                    PersistentTablePool<TKey, TValue>.Converters.RetrieveKeyColumn(
                        this.Session,
                        this.Table,
                        this.KeyColumn);

                var value =
                    (TValue)
                    PersistentTablePool<TKey, TValue>.Converters.RetrieveValueColumn(
                        this.Session,
                        this.Table,
                        this.ValueColumn);
                yield return new KeyValuePair<TKey, TValue>(key, value);
                --maxValues;
                hasNext = ESENT.Api.TryMoveNext(this.Session, this.Table);
            }
            while (hasNext && maxValues > 0);
        }

        /// <summary>
        /// The dispose.
        /// </summary>
        void IDisposable.Dispose()
        {
            var session = this.Session;
            if (session == null)
            {
                return;
            }

            try
            {
                if (session.JetSesid.IsInvalid)
                {
                    return;
                }

                ESENT.Api.JetSetSessionContext(session, this.Context);
                var table = this.Table;
                if (table != null)
                {
                    table.Close();
                    table.Dispose();
                    this.Table = null;
                }
                
                ESENT.Api.JetResetSessionContext(session);
                session.End();
                session.Dispose();
                this.Session = null;
            }
            finally
            {
                this.SessionHandle.Free();
            }
        }

        /// <summary>
        /// Updates the specified row with the specified value.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <param name="prep">The type of update.</param>
        private void UpdateRow(TKey key, TValue value, ESENT.JET_prep prep)
        {
            using (var update = new ESENT.Update(this.Session, this.Table, prep))
            {
                PersistentTablePool<TKey, TValue>.Converters.SetKeyColumn(this.Session, this.Table, this.KeyColumn, key);
                PersistentTablePool<TKey, TValue>.Converters.SetValueColumn(this.Session, this.Table, this.ValueColumn, value);
                update.Save();
            }
        }

        /// <summary>
        /// Retrieves the value for the provided key, or the default value if the key doesn't exist.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>The value.</returns>
        private TValue GetInternal(TKey key)
        {
            ESENT.Api.JetSetCurrentIndex(this.Session, this.Table, PersistentTableConstants.PrimaryIndexName);
            PersistentTablePool<TKey, TValue>.Converters.MakeKey(this.Session, this.Table, key, ESENT.MakeKeyGrbit.NewKey);
            
            if (ESENT.Api.TrySeek(this.Session, this.Table, ESENT.SeekGrbit.SeekEQ))
            {
                return (TValue)PersistentTablePool<TKey, TValue>.Converters.RetrieveValueColumn(this.Session, this.Table, this.ValueColumn);
            }

            return default(TValue);
        }
    }
}