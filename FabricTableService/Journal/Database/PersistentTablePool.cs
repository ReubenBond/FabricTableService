namespace FabricTableService.Journal.Database
{
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Isam.Esent.Interop.Windows7;
    using Microsoft.Isam.Esent.Interop.Windows8;
    using Microsoft.Isam.Esent.Interop.Windows81;

    public class PersistentTablePool<TKey, TValue> : IDisposable
    {
        /// <summary>
        /// The converters.
        /// </summary>
        internal static readonly DatabaseTypeConverters<TKey, TValue> Converters =
            new DatabaseTypeConverters<TKey, TValue>();

        /// <summary>
        /// The underlying collection of sessions.
        /// </summary>
        private readonly BlockingCollection<PersistentTable<TKey, TValue>> pool =
            new BlockingCollection<PersistentTable<TKey, TValue>>();

        /// <summary>
        /// The maximum pool size.
        /// </summary>
        private readonly int maxPoolSize;

        /// <summary>
        /// The number of instances which have been created.
        /// </summary>
        private int created;

        /// <summary>
        /// Initializes static members of the <see cref="PersistentTable{TKey,TValue}"/> class.
        /// </summary>
        static PersistentTablePool()
        {
            Microsoft.Isam.Esent.Interop.SystemParameters.MaxInstances = 1024;
        }

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
        /// <param name="maxPoolSize">
        /// The maximum size of the pool.
        /// </param>
        public PersistentTablePool(string directory, string databaseFile, string tableName, int maxPoolSize = int.MaxValue)
        {
            this.Directory = directory;
            this.DatabaseFile = databaseFile;
            this.TableName = tableName;
            this.maxPoolSize = maxPoolSize;
        }

        /// <summary>
        /// Gets the database file.
        /// </summary>
        public string Directory { get; }

        /// <summary>
        /// Gets the database file.
        /// </summary>
        public string DatabaseFile { get; }

        /// <summary>
        /// Gets the table name.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// The instance.
        /// </summary>
        internal Microsoft.Isam.Esent.Interop.Instance Instance { get; private set; }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        public void Initialize()
        {
            // Create the database & schema it if it does not yet exist.
            CreateDatabaseIfNotExists(this.Directory, this.DatabaseFile, this.TableName);

            // Initialize an instance of the database engine.
            this.Instance = new Microsoft.Isam.Esent.Interop.Instance("instance" + $"{this.GetHashCode():X}");
            this.Instance.Parameters.LogFileDirectory =
                this.Instance.Parameters.SystemDirectory =
                this.Instance.Parameters.TempDirectory =
                this.Instance.Parameters.AlternateDatabaseRecoveryDirectory = this.Directory;
            this.Instance.Parameters.CircularLog = true;
            this.Instance.Init();
        }

        public Task Backup(string destination)
        {
            if (!System.IO.Directory.Exists(destination))
            {
                System.IO.Directory.CreateDirectory(destination);
            }

            var completion = new TaskCompletionSource<int>();
            Microsoft.Isam.Esent.Interop.Api.JetBackupInstance(
                this.Instance,
                destination,
                Microsoft.Isam.Esent.Interop.BackupGrbit.Atomic,
                (sesid, snp, snt, data) =>
                {
                    var statusString = $"({sesid}, {snp}, {snt}, {data})";
                    switch (snt)
                    {
                        case Microsoft.Isam.Esent.Interop.JET_SNT.Begin:
                            Trace.TraceInformation("Began backup: " + statusString);
                            break;
                        case Microsoft.Isam.Esent.Interop.JET_SNT.Fail:
                            Trace.TraceInformation("Failed backup: " + statusString);
                            completion.SetException(new Exception("Backup operation failed: " + statusString));
                            break;
                        case Microsoft.Isam.Esent.Interop.JET_SNT.Complete:
                            Trace.TraceInformation("Completed backup: " + statusString);
                            completion.SetResult(0);
                            break;
                        case Microsoft.Isam.Esent.Interop.JET_SNT.RecoveryStep:
                            Trace.TraceInformation("Recovery step during backup: " + statusString);
                            break;
                    }

                    return Microsoft.Isam.Esent.Interop.JET_err.Success;
                });
            return completion.Task;
        }

        public Task Restore(string source, string destination)
        {
            // Initialize an instance of the database engine.
            this.Instance = new Microsoft.Isam.Esent.Interop.Instance("instance" + $"{this.GetHashCode():X}");
            this.Instance.Parameters.LogFileDirectory =
                this.Instance.Parameters.SystemDirectory =
                this.Instance.Parameters.TempDirectory =
                this.Instance.Parameters.AlternateDatabaseRecoveryDirectory = this.Directory;
            this.Instance.Parameters.CircularLog = true;
            var completion = new TaskCompletionSource<int>();
            Microsoft.Isam.Esent.Interop.Api.JetRestoreInstance(
                this.Instance,
                source,
                destination,
                (sesid, snp, snt, data) =>
                {
                    var statusString = $"({sesid}, {snp}, {snt}, {data})";
                    switch (snt)
                    {
                        case Microsoft.Isam.Esent.Interop.JET_SNT.Begin:
                            Trace.TraceInformation("Began restore: " + statusString);
                            break;
                        case Microsoft.Isam.Esent.Interop.JET_SNT.Fail:
                            Trace.TraceInformation("Failed restore: " + statusString);
                            completion.SetException(new Exception("Restore operation failed: " + statusString));
                            break;
                        case Microsoft.Isam.Esent.Interop.JET_SNT.Complete:
                            Trace.TraceInformation("Completed restore: " + statusString);
                            completion.SetResult(0);
                            break;
                        case Microsoft.Isam.Esent.Interop.JET_SNT.RecoveryStep:
                            Trace.TraceInformation("Recovery step during restore: " + statusString);
                            break;
                    }

                    return Microsoft.Isam.Esent.Interop.JET_err.Success;
                });
            return completion.Task;
        }

        public PersistentTable<TKey, TValue> Take()
        {
            PersistentTable<TKey, TValue> result;
            if (!this.pool.TryTake(out result))
            {
                if (Interlocked.Increment(ref this.created) < this.maxPoolSize)
                {
                    result = this.CreateInstance();
                }
                else
                {
                    // Undo what we just did and wait for a free instance.
                    Interlocked.Decrement(ref this.created);
                    this.pool.Take();
                }
            }

            return result;
        }

        public void Return(PersistentTable<TKey, TValue> table)
        {
            this.pool.Add(table);
        }

        private PersistentTable<TKey, TValue> CreateInstance()
        {
            var session = new Microsoft.Isam.Esent.Interop.Session(this.Instance);

            // Open the database.
            var database = OpenDatabase(session, Path.Combine(this.Directory, this.DatabaseFile));

            // Get a reference to the table.
            var table = new Microsoft.Isam.Esent.Interop.Table(session, database, this.TableName, Microsoft.Isam.Esent.Interop.OpenTableGrbit.None);

            // Get references to the columns.
            var columns = Microsoft.Isam.Esent.Interop.Api.GetColumnDictionary(session, table);

            return new PersistentTable<TKey, TValue>
            {
                Table = table,
                Session = session,
                KeyColumn = columns["key"],
                ValueColumn = columns["value"],
                SessionHandle = GCHandle.Alloc(session)
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
        /// The <see cref="Microsoft.Isam.Esent.Interop.JET_DBID"/>.
        /// </returns>
        private static Microsoft.Isam.Esent.Interop.JET_DBID OpenDatabase(Microsoft.Isam.Esent.Interop.Session session, string databaseFile)
        {
            Trace.TraceInformation($"Opening database '{databaseFile}'.");
            Microsoft.Isam.Esent.Interop.JET_DBID database;
            Microsoft.Isam.Esent.Interop.Api.JetAttachDatabase(session, databaseFile, Microsoft.Isam.Esent.Interop.AttachDatabaseGrbit.None);
            Microsoft.Isam.Esent.Interop.Api.JetOpenDatabase(session, databaseFile, null, out database, Microsoft.Isam.Esent.Interop.OpenDatabaseGrbit.None);

            Trace.TraceInformation("Successfully opened database.");
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
                Trace.TraceInformation(
                    "Creating database '{0}' with table '{1}' in directory '{2}'.",
                    databaseFile,
                    tableName,
                    directory);
                CreateDatabase(directory, databaseFile, tableName);
                Trace.TraceInformation("Successfully created database.");
            }
            else
            {
                Trace.TraceInformation($"Database '{databaseFile}' exists.");
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
            using (var instance = new Microsoft.Isam.Esent.Interop.Instance("createdatabase" + Guid.NewGuid().ToString("N")))
            {
                instance.Parameters.LogFileDirectory = directory;
                instance.Parameters.SystemDirectory = directory;
                instance.Parameters.TempDirectory = directory;
                instance.Init();
                using (var session = new Microsoft.Isam.Esent.Interop.Session(instance))
                {
                    Microsoft.Isam.Esent.Interop.JET_DBID database;
                    Microsoft.Isam.Esent.Interop.Api.JetCreateDatabase(
                        session,
                        Path.Combine(directory, databaseFile),
                        null,
                        out database,
                        Microsoft.Isam.Esent.Interop.CreateDatabaseGrbit.OverwriteExisting);
                    using (var tx = new Microsoft.Isam.Esent.Interop.Transaction(session))
                    {
                        Microsoft.Isam.Esent.Interop.JET_TABLEID table;
                        Trace.TraceInformation($"Creating table '{tableName}'");
                        Microsoft.Isam.Esent.Interop.Api.JetCreateTable(session, database, tableName, 16, 100, out table);
                        CreateColumnsAndIndexes(session, table);
                        Microsoft.Isam.Esent.Interop.Api.JetCloseTable(session, table);
                        tx.Commit(Microsoft.Isam.Esent.Interop.CommitTransactionGrbit.None);
                    }
                    
                    Trace.TraceInformation($"Created table '{tableName}'.");
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
        private static void CreateColumnsAndIndexes(Microsoft.Isam.Esent.Interop.Session session, Microsoft.Isam.Esent.Interop.JET_TABLEID table)
        {
            Microsoft.Isam.Esent.Interop.JET_COLUMNID column;
            var keyDefinition = new Microsoft.Isam.Esent.Interop.JET_COLUMNDEF { coltyp = Converters.KeyColtyp };
            var valueDefinition = new Microsoft.Isam.Esent.Interop.JET_COLUMNDEF { coltyp = Converters.ValueColtyp };

            if (typeof(TValue) == typeof(string))
            {
                valueDefinition.cp = Microsoft.Isam.Esent.Interop.JET_CP.Unicode;
                valueDefinition.cbMax = int.MaxValue;
            }

            if (typeof(TKey) == typeof(string))
            {
                keyDefinition.cp = Microsoft.Isam.Esent.Interop.JET_CP.Unicode;
                keyDefinition.cbMax = int.MaxValue;
            }

            // Add a key and a value column.
            Microsoft.Isam.Esent.Interop.Api.JetAddColumn(session, table, "key", keyDefinition, null, 0, out column);
            Microsoft.Isam.Esent.Interop.Api.JetAddColumn(session, table, "value", valueDefinition, null, 0, out column);

            // Create the primary index.
            const string PrimaryIndexDefinition = "+key\0\0";
            Microsoft.Isam.Esent.Interop.Api.JetCreateIndex(
                session,
                table,
                PersistentTableConstants.PrimaryIndexName,
                Microsoft.Isam.Esent.Interop.CreateIndexGrbit.IndexPrimary,
                PrimaryIndexDefinition,
                PrimaryIndexDefinition.Length,
                100);
        }

        void IDisposable.Dispose()
        {
            PersistentTable<TKey, TValue> table;
            while (this.pool.TryTake(out table))
            {
                ((IDisposable)table).Dispose();
            }

            var instance = this.Instance;
            if (instance == null)
            {
                return;
            }

            instance.TermGrbit = Microsoft.Isam.Esent.Interop.TermGrbit.Complete;
            instance.Term();
            instance.Dispose();
            this.Instance = null;
        }
    }
}