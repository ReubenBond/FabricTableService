namespace FabricTableService.Journal
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Vista;

    /// <summary>
    /// The esent sample.
    /// </summary>
    public class PersistentTable : IDisposable
    {
        /// <summary>
        /// The primary index name.
        /// </summary>
        private const string PrimaryIndexName = "primary";

        /// <summary>
        /// The maximum possible <see cref="Guid"/> value.
        /// </summary>
        private static readonly Guid MaxGuid = Guid.Parse("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF");

        /// <summary>
        /// The key column.
        /// </summary>
        private JET_COLUMNID keyColumn;

        /// <summary>
        /// The value column.
        /// </summary>
        private JET_COLUMNID valueColumn;

        /// <summary>
        /// The table.
        /// </summary>
        private Table table;

        /// <summary>
        /// The session.
        /// </summary>
        private Session session;

        /// <summary>
        /// The instance.
        /// </summary>
        private Instance instance;

        static PersistentTable()
        {
            SystemParameters.MaxInstances = 1024;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PersistentTable"/> class.
        /// </summary>
        /// <param name="databaseFile">
        /// The database file.
        /// </param>
        /// <param name="tableName">
        /// The table name.
        /// </param>
        public PersistentTable(string directory, string databaseFile, string tableName)
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
        /// Gets the session.
        /// </summary>
        internal JET_SESID Session
        {
            get
            {
                return this.session;
            }
        }

        /// <summary>
        /// Gets the table.
        /// </summary>
        internal JET_TABLEID Table
        {
            get
            {
                return this.table;
            }
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        public void Initialize()
        {
            // Create the database & schema it if it does not yet exist.
            CreateDatabaseIfNotExists(this.Directory, this.DatabaseFile, this.TableName);

            // Initialize an instance of the database engine.
            this.instance = new Instance("instance" + string.Format("{0:X}", this.GetHashCode()));
            this.instance.Parameters.LogFileDirectory =
                this.instance.Parameters.SystemDirectory = this.instance.Parameters.TempDirectory = this.Directory;
            this.instance.Parameters.CircularLog = true;
            this.instance.Init();

            // Start a new session.
            this.session = new Session(this.instance);

            // Open the database.
            var database = OpenDatabase(this.session, Path.Combine(this.Directory, this.DatabaseFile));

            // Get a reference to the table.
            this.table = this.GetTable(database);

            // Get references to the columns.
            var columns = Api.GetColumnDictionary(this.session, this.table);
            this.keyColumn = columns["key"];
            this.valueColumn = columns["value"];
        }

        public Task Backup(string destination)
        {
            var completion = new TaskCompletionSource<int>();
            Api.JetBackupInstance(
                this.instance,
                destination,
                BackupGrbit.Atomic,
                (sesid, snp, snt, data) =>
                {
                    var statusString = string.Format("({0}, {1}, {2}, {3})", sesid, snp, snt, data);
                    switch (snt)
                    {
                        case JET_SNT.Begin:
                            Debug.WriteLine("Began backup: " + statusString);
                            break;
                        case JET_SNT.Fail:
                            Debug.WriteLine("Failed backup: " + statusString);
                            completion.SetException(new Exception("Backup operation failed: " + statusString));
                            break;
                        case JET_SNT.Complete:
                            Debug.WriteLine("Completed backup: " + statusString);
                            completion.SetResult(0);
                            break;
                        case JET_SNT.RecoveryStep:
                            Debug.WriteLine("Recovery step during backup: " + statusString);
                            break;
                    }

                    return JET_err.Success;
                });
            return completion.Task;
        }

        public Task Restore(string source, string destination)
        {
            var completion = new TaskCompletionSource<int>();
            Api.JetRestoreInstance(
                this.instance,
                source,
                destination,
                (sesid, snp, snt, data) =>
                {
                    var statusString = string.Format("({0}, {1}, {2}, {3})", sesid, snp, snt, data);
                    switch (snt)
                    {
                        case JET_SNT.Begin:
                            Debug.WriteLine("Began restore: " + statusString);
                            break;
                        case JET_SNT.Fail:
                            Debug.WriteLine("Failed restore: " + statusString);
                            completion.SetException(new Exception("Restore operation failed: " + statusString));
                            break;
                        case JET_SNT.Complete:
                            Debug.WriteLine("Completed restore: " + statusString);
                            completion.SetResult(0);
                            break;
                        case JET_SNT.RecoveryStep:
                            Debug.WriteLine("Recovery step during restore: " + statusString);
                            break;
                    }

                    return JET_err.Success;
                });
            return completion.Task;
        }
/*
        /// <summary>
        /// The run.
        /// </summary>
        public void Run()
        {
            using (var tx = new Transaction(this))
            {
                Debug.WriteLine("Inserting values into database");
                this.AddOrUpdate(Guid.NewGuid(), "SO RANDOM!!! " + Guid.NewGuid());
                tx.Commit();
            }

            // Retrieve a column from the record. Here we move to the first record with JetMove. By using
            // JetMoveNext it is possible to iterate through all records in a table. Use JetMakeKey and
            // JetSeek to move to a particular record.
            Api.JetMove(this.session, this.table, JET_Move.First, MoveGrbit.None);
            var value = Api.RetrieveColumnAsString(this.session, this.table, this.valueColumn, Encoding.Unicode);
            Debug.WriteLine("First value: {0}", value);

            Debug.WriteLine("All rows:");
            Api.MoveBeforeFirst(this.session, this.table);
            while (Api.TryMoveNext(this.session, this.table))
            {
                var key = Api.RetrieveColumnAsGuid(this.session, this.table, this.keyColumn);
                if (!key.HasValue)
                {
                    continue;
                }

                var val = Api.RetrieveColumnAsString(
                    this.session,
                    this.table,
                    this.valueColumn,
                    Encoding.Unicode);
                var row = new KeyValuePair<Guid, string>(key.Value, val);

                Debug.WriteLine("{0} => '{1}'", row.Key, row.Value);
            }

            Debug.WriteLine("First N rows");
            foreach (var row in this.GetRange())
            {
                Debug.WriteLine("{0} => '{1}'", row.Key, row.Value);
            }
        }*/

        public bool TryGetValue(Guid key, out string value)
        {
            value = this.GetInternal(key);
            return value != null;
        }

        public string AddOrUpdate(Guid key, Func<Guid, string> addFunc, Func<Guid, string, string> updateFunc)
        {
            var existing = this.GetInternal(key);
            string newValue;
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
        public void AddOrUpdate(Guid key, string value)
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

        public string AddOrUpdate(Guid key, string value, Func<Guid, string, string> updateFunc)
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

        public string GetOrAdd(Guid key, Func<Guid, string> addFunc)
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

        public string GetOrAdd(Guid key, string value)
        {
            var existing = this.GetInternal(key);
            if (existing != null)
            {
                return existing;
            }

            this.Add(key, value);
            return value;
        }

        public void Update(Guid key, string value)
        {
            this.UpdateRow(key, value, JET_prep.Replace);
        }

        public void Add(Guid key, string value)
        {
            this.UpdateRow(key, value, JET_prep.Insert);
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
        public bool Contains(Guid key)
        {
            Api.JetSetCurrentIndex(this.session, this.table, PrimaryIndexName);
            Api.MakeKey(this.session, this.table, key, MakeKeyGrbit.NewKey);

            if (!Api.TrySeek(this.session, this.table, SeekGrbit.SeekEQ))
            {
                return false;
            }

            return true;
        }

        public string Get(Guid key)
        {
            string value;
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
        public void Remove(Guid key)
        {
            string value;
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
        /// <returns>
        /// <see langword="true"/> if the key will be deleted when the transaction commits, <see langword="false"/> otherwise.
        /// </returns>
        public bool TryRemove(Guid key, out string value)
        {
            Api.JetSetCurrentIndex(this.session, this.table, PrimaryIndexName);
            Api.MakeKey(this.session, this.table, key, MakeKeyGrbit.NewKey);

            if (!Api.TrySeek(this.session, this.table, SeekGrbit.SeekEQ))
            {
                value = null;
                return false;
            }

            value = Api.RetrieveColumnAsString(this.session, this.table, this.valueColumn, Encoding.Unicode);
            Api.JetDelete(this.session, this.table);
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
        public IEnumerable<KeyValuePair<Guid, string>> GetRange(
            Guid? lowerBound = default(Guid?),
            Guid? upperBound = default(Guid?),
            long maxValues = long.MaxValue)
        {
            var lower = lowerBound ?? Guid.Empty;
            var upper = upperBound ?? MaxGuid;

            // Set the index and seek to the lower bound, if it has been specified.
            Api.JetSetCurrentIndex(this.session, this.table, PrimaryIndexName);
            Api.MakeKey(this.session, this.table, lower, MakeKeyGrbit.NewKey);
            if (!Api.TrySeek(this.session, this.table, SeekGrbit.SeekGE))
            {
                yield break;
            }

            // Set the upper limit of the index scan.
            Api.MakeKey(this.session, this.table, upper, MakeKeyGrbit.NewKey | MakeKeyGrbit.FullColumnEndLimit);
            const SetIndexRangeGrbit RangeFlags = SetIndexRangeGrbit.RangeInclusive | SetIndexRangeGrbit.RangeUpperLimit;
            if (!Api.TrySetIndexRange(this.session, this.table, RangeFlags))
            {
                yield break;
            }

            // Iterate over the ranged index.
            var hasNext = false;
            do
            {
                var key = Api.RetrieveColumnAsGuid(this.session, this.table, this.keyColumn);
                if (!key.HasValue)
                {
                    continue;
                }

                var value = Api.RetrieveColumnAsString(this.session, this.table, this.valueColumn, Encoding.Unicode);
                yield return new KeyValuePair<Guid, string>(key.Value, value);
                --maxValues;
                hasNext = Api.TryMoveNext(this.session, this.table);
            }
            while (hasNext && maxValues > 0);
        }

        /// <summary>
        /// The dispose.
        /// </summary>
        public void Dispose()
        {
            var tbl = this.table;
            if (tbl != null)
            {
                tbl.Dispose();
                this.table = null;
            }

            var ses = this.session;
            if (ses != null)
            {
                ses.Dispose();
                this.session = null;
            }

            var inst = this.instance;
            if (inst != null)
            {
                inst.Dispose();
                this.instance = null;
            }
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
        /// The <see cref="JET_DBID"/>.
        /// </returns>
        private static JET_DBID OpenDatabase(Session session, string databaseFile)
        {
            Debug.WriteLine("Opening database '{0}'.", databaseFile);
            JET_DBID database;
            Api.JetAttachDatabase(session, databaseFile, AttachDatabaseGrbit.None);
            Api.JetOpenDatabase(session, databaseFile, null, out database, OpenDatabaseGrbit.None);

            Debug.WriteLine("Successfully opened database.");
            return database;
        }

        /// <summary>
        /// The create database if not exists.
        /// </summary>
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
                Debug.WriteLine("Database '{0}' exists.", databaseFile);
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
            using (var instance = new Instance("createdatabase" + Guid.NewGuid().ToString("N")))
            {
                instance.Parameters.LogFileDirectory = directory;
                instance.Parameters.SystemDirectory = directory;
                instance.Parameters.TempDirectory = directory;
                instance.Init();
                using (var session = new Session(instance))
                {
                    JET_DBID database;
                    Api.JetCreateDatabase(
                        session,
                        Path.Combine(directory, databaseFile),
                        null,
                        out database,
                        CreateDatabaseGrbit.OverwriteExisting);
                    using (var tx = new Microsoft.Isam.Esent.Interop.Transaction(session))
                    {
                        JET_TABLEID table;
                        Debug.WriteLine("Creating table '{0}'", tableName);
                        Api.JetCreateTable(session, database, tableName, 16, 100, out table);
                        CreateColumnsAndIndexes(session, table);
                        Api.JetCloseTable(session, table);
                        tx.Commit(CommitTransactionGrbit.None);
                    }

                    Debug.WriteLine("Created table '{0}'.", tableName);
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
        private static void CreateColumnsAndIndexes(Session session, JET_TABLEID table)
        {
            using (var tx = new Microsoft.Isam.Esent.Interop.Transaction(session))
            {
                JET_COLUMNID column;
                var keyDefinition = new JET_COLUMNDEF { coltyp = VistaColtyp.GUID };
                var valueDefinition = new JET_COLUMNDEF { coltyp = JET_coltyp.LongText, cp = JET_CP.Unicode };

                // Add a key and a value column.
                Api.JetAddColumn(session, table, "key", keyDefinition, null, 0, out column);
                Api.JetAddColumn(session, table, "value", valueDefinition, null, 0, out column);

                // Create the primary index.
                const string PrimaryIndexDefinition = "+key\0\0";
                Api.JetCreateIndex(
                    session,
                    table,
                    PrimaryIndexName,
                    CreateIndexGrbit.IndexPrimary,
                    PrimaryIndexDefinition,
                    PrimaryIndexDefinition.Length,
                    100);

                tx.Commit(CommitTransactionGrbit.None);
            }
        }

        /// <summary>
        /// The get table.
        /// </summary>
        /// <param name="database">
        /// The database.
        /// </param>
        /// <returns>
        /// The <see cref="Table"/>.
        /// </returns>
        private Table GetTable(JET_DBID database)
        {
            return new Table(this.session, database, this.TableName, OpenTableGrbit.None);
        }

        private void UpdateRow(Guid key, string value, JET_prep prep)
        {
            using (var update = new Update(this.session, this.table, prep))
            {
                Api.SetColumn(this.session, this.table, this.keyColumn, key);
                Api.SetColumn(this.session, this.table, this.valueColumn, value, Encoding.Unicode);
                update.Save();
            }
        }

        private string GetInternal(Guid key)
        {
            Api.JetSetCurrentIndex(this.session, this.table, PrimaryIndexName);
            Api.MakeKey(this.session, this.table, key, MakeKeyGrbit.NewKey);

            if (Api.TrySeek(this.session, this.table, SeekGrbit.SeekEQ))
            {
                return Api.RetrieveColumnAsString(this.session, this.table, this.valueColumn, Encoding.Unicode);
            }

            return null;
        }
    }
}