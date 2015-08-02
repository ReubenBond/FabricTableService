namespace FabricTableService.Journal.Database
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading.Tasks;

    using ESENT = Microsoft.Isam.Esent.Interop;

    internal class PersistentTableConstants
    {
        /// <summary>
        /// The primary index name.
        /// </summary>
        internal const string PrimaryIndexName = "primary";
    }

    public class PersistentTablePool<TKey, TValue> : IDisposable
    {
        private readonly ConcurrentBag<PersistentTable<TKey, TValue>> pool =
            new ConcurrentBag<PersistentTable<TKey, TValue>>();
        
        /// <summary>
        /// Initializes static members of the <see cref="PersistentTable{TKey,TValue}"/> class.
        /// </summary>
        static PersistentTablePool()
        {
            ESENT.SystemParameters.MaxInstances = 1024;
        }

        /// <summary>
        /// The converters.
        /// </summary>
        internal static readonly DatabaseTypeConverters<TKey, TValue> Converters = new DatabaseTypeConverters<TKey, TValue>();
        
        /// <summary>
        /// Initializes a new instance of the <see cref="PersistentTable{TKey,TValue}"/> class.
        /// </summary>
        /// <param name="directory">
        /// The database directory.
        /// </param>
        /// <param name="databaseFile">
        /// The database file.
        /// </param>
        /// <param name="tableName">
        /// The table name.
        /// </param>
        public PersistentTablePool(string directory, string databaseFile, string tableName)
        {
            this.Directory = directory;
            this.DatabaseFile = databaseFile;
            this.TableName = tableName;
        }

        /// <summary>
        /// Gets the database file.
        /// </summary>
        public string Directory { get; private set; }

        /// <summary>
        /// Gets the database file.
        /// </summary>
        public string DatabaseFile { get; private set; }

        /// <summary>
        /// Gets the table name.
        /// </summary>
        public string TableName { get; private set; }
        
        /// <summary>
        /// The instance.
        /// </summary>
        internal ESENT.Instance Instance { get; private set; }
        
        /// <summary>
        /// Initializes this instance.
        /// </summary>
        public void Initialize()
        {
            // Create the database & schema it if it does not yet exist.
            CreateDatabaseIfNotExists(this.Directory, this.DatabaseFile, this.TableName);

            // Initialize an instance of the database engine.
            this.Instance = new ESENT.Instance("instance" + string.Format("{0:X}", this.GetHashCode()));
            this.Instance.Parameters.LogFileDirectory =
                this.Instance.Parameters.SystemDirectory =
                this.Instance.Parameters.TempDirectory =
                this.Instance.Parameters.AlternateDatabaseRecoveryDirectory = this.Directory;
            this.Instance.Parameters.CircularLog = true;
            this.Instance.Init();
        }

        public Task Backup(string destination)
        {
            var completion = new TaskCompletionSource<int>();
            ESENT.Api.JetBackupInstance(
                this.Instance,
                destination,
                ESENT.BackupGrbit.Atomic,
                (sesid, snp, snt, data) =>
                {
                    var statusString = string.Format("({0}, {1}, {2}, {3})", sesid, snp, snt, data);
                    switch (snt)
                    {
                        case ESENT.JET_SNT.Begin:
                            Debug.WriteLine("Began backup: " + statusString);
                            break;
                        case ESENT.JET_SNT.Fail:
                            Debug.WriteLine("Failed backup: " + statusString);
                            completion.SetException(new Exception("Backup operation failed: " + statusString));
                            break;
                        case ESENT.JET_SNT.Complete:
                            Debug.WriteLine("Completed backup: " + statusString);
                            completion.SetResult(0);
                            break;
                        case ESENT.JET_SNT.RecoveryStep:
                            Debug.WriteLine("Recovery step during backup: " + statusString);
                            break;
                    }

                    return ESENT.JET_err.Success;
                });
            return completion.Task;
        }

        public Task Restore(string source, string destination)
        {
            var completion = new TaskCompletionSource<int>();
            ESENT.Api.JetRestoreInstance(
                this.Instance,
                source,
                destination,
                (sesid, snp, snt, data) =>
                {
                    var statusString = string.Format("({0}, {1}, {2}, {3})", sesid, snp, snt, data);
                    switch (snt)
                    {
                        case ESENT.JET_SNT.Begin:
                            Debug.WriteLine("Began restore: " + statusString);
                            break;
                        case ESENT.JET_SNT.Fail:
                            Debug.WriteLine("Failed restore: " + statusString);
                            completion.SetException(new Exception("Restore operation failed: " + statusString));
                            break;
                        case ESENT.JET_SNT.Complete:
                            Debug.WriteLine("Completed restore: " + statusString);
                            completion.SetResult(0);
                            break;
                        case ESENT.JET_SNT.RecoveryStep:
                            Debug.WriteLine("Recovery step during restore: " + statusString);
                            break;
                    }

                    return ESENT.JET_err.Success;
                });
            return completion.Task;
        }

        public PersistentTable<TKey, TValue> Take()
        {
            PersistentTable<TKey, TValue> result;
            if (!this.pool.TryTake(out result))
            {
                result = this.CreateInstance();
            }

            return result;
        }

        public void Return(PersistentTable<TKey, TValue> table)
        {
            this.pool.Add(table);
        }

        private PersistentTable<TKey, TValue> CreateInstance()
        {
            var session = new ESENT.Session(this.Instance);

            // Open the database.
            var database = OpenDatabase(session, Path.Combine(this.Directory, this.DatabaseFile));

            // Get a reference to the table.
            var table = new ESENT.Table(session, database, this.TableName, ESENT.OpenTableGrbit.None);

            // Get references to the columns.
            var columns = ESENT.Api.GetColumnDictionary(session, table);

            return new PersistentTable<TKey, TValue>
            {
                Table = table,
                Session = new ESENT.Session(this.Instance),
                KeyColumn = columns["key"],
                ValueColumn = columns["value"]
            };
        }

        /// <summary>
        /// The open database.
        /// </summary>
        /// <param name="session">
        /// The session.
        /// </param>
        /// <param name="databaseFile">
        /// The database file.
        /// </param>
        /// <returns>
        /// The <see cref="ESENT.JET_DBID"/>.
        /// </returns>
        private static ESENT.JET_DBID OpenDatabase(ESENT.Session session, string databaseFile)
        {
            Debug.WriteLine(string.Format("Opening database '{0}'.", databaseFile));
            ESENT.JET_DBID database;
            ESENT.Api.JetAttachDatabase(session, databaseFile, ESENT.AttachDatabaseGrbit.None);
            ESENT.Api.JetOpenDatabase(session, databaseFile, null, out database, ESENT.OpenDatabaseGrbit.None);

            Debug.WriteLine("Successfully opened database.");
            return database;
        }

        /// <summary>
        /// The create database if not exists.
        /// </summary>
        /// <param name="directory">
        /// The directory in which the database exists.
        /// </param>
        /// <param name="databaseFile">
        /// The database file.
        /// </param>
        /// <param name="tableName">
        /// The table name.
        /// </param>
        private static void CreateDatabaseIfNotExists(string directory, string databaseFile, string tableName)
        {
            if (!File.Exists(databaseFile))
            {
                Debug.WriteLine("Creating database '{0}' with table '{1}' in directory '{2}'.", databaseFile, tableName, directory);
                CreateDatabase(directory, databaseFile, tableName);
                Debug.WriteLine("Successfully created database.");
            }
            else
            {
                Debug.WriteLine(string.Format("Database '{0}' exists.", databaseFile));
            }
        }

        /// <summary>
        /// The create database.
        /// </summary>
        /// <param name="directory">
        /// The database file directory.
        /// </param>
        /// <param name="databaseFile">
        /// The database file, excluding the directory.
        /// </param>
        /// <param name="tableName">
        /// The table name.
        /// </param>
        private static void CreateDatabase(string directory, string databaseFile, string tableName)
        {
            using (var instance = new ESENT.Instance("createdatabase" + Guid.NewGuid().ToString("N")))
            {
                instance.Parameters.LogFileDirectory = directory;
                instance.Parameters.SystemDirectory = directory;
                instance.Parameters.TempDirectory = directory;
                instance.Init();
                using (var session = new ESENT.Session(instance))
                {
                    ESENT.JET_DBID database;
                    ESENT.Api.JetCreateDatabase(
                        session,
                        Path.Combine(directory, databaseFile),
                        null,
                        out database,
                        ESENT.CreateDatabaseGrbit.OverwriteExisting);
                    using (var tx = new ESENT.Transaction(session))
                    {
                        ESENT.JET_TABLEID table;
                        Debug.WriteLine(string.Format("Creating table '{0}'", tableName));
                        ESENT.Api.JetCreateTable(session, database, tableName, 16, 100, out table);
                        CreateColumnsAndIndexes(session, table);
                        ESENT.Api.JetCloseTable(session, table);
                        tx.Commit(ESENT.CommitTransactionGrbit.None);
                    }

                    Debug.WriteLine(string.Format("Created table '{0}'.", tableName));
                }
            }
        }

        /// <summary>
        /// The create columns and indexes.
        /// </summary>
        /// <param name="session">
        /// The session.
        /// </param>
        /// <param name="table">
        /// The table.
        /// </param>
        private static void CreateColumnsAndIndexes(ESENT.Session session, ESENT.JET_TABLEID table)
        {
            using (var tx = new ESENT.Transaction(session))
            {
                ESENT.JET_COLUMNID column;
                var keyDefinition = new ESENT.JET_COLUMNDEF { coltyp = Converters.KeyColtyp };
                var valueDefinition = new ESENT.JET_COLUMNDEF { coltyp = Converters.ValueColtyp };

                // Add a key and a value column.
                ESENT.Api.JetAddColumn(session, table, "key", keyDefinition, null, 0, out column);
                ESENT.Api.JetAddColumn(session, table, "value", valueDefinition, null, 0, out column);

                // Create the primary index.
                const string PrimaryIndexDefinition = "+key\0\0";
                ESENT.Api.JetCreateIndex(
                    session,
                    table,
                    PersistentTableConstants.PrimaryIndexName,
                    ESENT.CreateIndexGrbit.IndexPrimary,
                    PrimaryIndexDefinition,
                    PrimaryIndexDefinition.Length,
                    100);

                tx.Commit(ESENT.CommitTransactionGrbit.None);
            }
        }

        void IDisposable.Dispose()
        {
            var inst = this.Instance;
            if (inst != null)
            {
                inst.Dispose();
                this.Instance = null;
            }

            PersistentTable<TKey, TValue> table;
            while (this.pool.TryTake(out table))
            {
                ((IDisposable)table).Dispose();
            }
        }
    }

    /// <summary>
    /// The esent sample.
    /// </summary>
    public class PersistentTable<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// The converters.
        /// </summary>
        internal static readonly DatabaseTypeConverters<TKey, TValue> Converters = new DatabaseTypeConverters<TKey, TValue>();

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
            Converters.MakeKey(
                this.Session,
                this.Table,
                key,
                ESENT.MakeKeyGrbit.NewKey);

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
            PersistentTablePool<TKey, TValue>.Converters.MakeKey(this.Session, this.Table, lowerBound, ESENT.MakeKeyGrbit.NewKey);
            if (!ESENT.Api.TrySeek(this.Session, this.Table, ESENT.SeekGrbit.SeekGE))
            {
                yield break;
            }

            // Set the upper limit of the index scan.
            PersistentTablePool<TKey, TValue>.Converters.MakeKey(this.Session, this.Table, upperBound, ESENT.MakeKeyGrbit.NewKey | ESENT.MakeKeyGrbit.FullColumnEndLimit);
            const ESENT.SetIndexRangeGrbit RangeFlags = ESENT.SetIndexRangeGrbit.RangeInclusive | ESENT.SetIndexRangeGrbit.RangeUpperLimit;
            if (!ESENT.Api.TrySetIndexRange(this.Session, this.Table, RangeFlags))
            {
                yield break;
            }

            // Iterate over the ranged index.
            bool hasNext;
            do
            {
                var key = (TKey)PersistentTablePool<TKey, TValue>.Converters.RetrieveKeyColumn(this.Session, this.Table, this.KeyColumn);

                var value = (TValue)PersistentTablePool<TKey, TValue>.Converters.RetrieveValueColumn(this.Session, this.Table, this.ValueColumn);
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
            var tbl = this.Table;
            if (tbl != null)
            {
                tbl.Dispose();
                this.Table = null;
            }

            var ses = this.Session;
            if (ses != null)
            {
                ses.Dispose();
                this.Session = null;
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
                Converters.SetKeyColumn(this.Session, this.Table, this.KeyColumn, key);
                Converters.SetValueColumn(this.Session, this.Table, this.ValueColumn, value);
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






            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////

            // ESENT SESSION handles must be used on the thread they were created on.
            // So we probably need to ensure that a transaction starts and finishes on the same thread and that each thread has a single ESENT session.
            // Plan: Turn PersistentTable into an object which schedules public methods on a given thread-bound scheduler.

            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////
            ////







            ESENT.Api.JetSetCurrentIndex(this.Session, this.Table, PersistentTableConstants.PrimaryIndexName);
            Converters.MakeKey(this.Session, this.Table, key, ESENT.MakeKeyGrbit.NewKey);

            if (ESENT.Api.TrySeek(this.Session, this.Table, ESENT.SeekGrbit.SeekEQ))
            {
                return (TValue)Converters.RetrieveValueColumn(this.Session, this.Table, this.ValueColumn);
            }

            return default(TValue);
        }
    }
}