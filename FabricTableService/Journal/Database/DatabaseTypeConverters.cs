// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DatabaseTypeConverters.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   A dictionary can contain many types of columns and exposes a strongly typed
//   interface. This code maps between .NET types and functions to set and retrieve
//   data in a PersistentDictionary.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace FabricTableService.Journal.Database
{
    using Microsoft.Isam.Esent.Interop;
    
    /// <summary>
    /// Contains methods to set and get data from the ESENT
    /// database.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the value.</typeparam>
    internal class DatabaseTypeConverters<TKey, TValue>
    {
        /// <summary>
        /// Column converter for the key column.
        /// </summary>
        private readonly KeyColumnConverter key;

        /// <summary>
        /// Column converter for the value column.
        /// </summary>
        private readonly ColumnConverter value;

        /// <summary>
        /// Initializes a new instance of the DatabaseTypeConverters
        /// class. This looks up the conversion types for
        /// <typeparamref name="TKey"/> and <typeparamref name="TValue"/>.
        /// </summary>
        public DatabaseTypeConverters()
        {
            this.key = new KeyColumnConverter(typeof(TKey));
            this.value = new ColumnConverter(typeof(TValue));
        }

        /// <summary>
        /// Gets a delegate that can be used to call JetMakeKey with an object of
        /// type <typeparamref name="TKey"/>.
        /// </summary>
        public KeyColumnConverter.MakeKeyDelegate MakeKey => this.key.MakeKey;

        /// <summary>
        /// Gets a delegate that can be used to set the Key column with an object of
        /// type <typeparamref name="TKey"/>.
        /// </summary>
        public ColumnConverter.SetColumnDelegate SetKeyColumn => this.key.SetColumn;

        /// <summary>
        /// Gets a delegate that can be used to set the Value column with an object of
        /// type <typeparamref name="TValue"/>.
        /// </summary>
        public ColumnConverter.SetColumnDelegate SetValueColumn => this.value.SetColumn;

        /// <summary>
        /// Gets a delegate that can be used to retrieve the Key column, returning
        /// an object of type <typeparamref name="TKey"/>.
        /// </summary>
        public ColumnConverter.RetrieveColumnDelegate RetrieveKeyColumn => this.key.RetrieveColumn;

        /// <summary>
        /// Gets a delegate that can be used to retrieve the Value column, returning
        /// an object of type <typeparamref name="TValue"/>.
        /// </summary>
        public ColumnConverter.RetrieveColumnDelegate RetrieveValueColumn => this.value.RetrieveColumn;

        /// <summary>
        /// Gets the JET_coltyp that the key column should have.
        /// </summary>
        public JET_coltyp KeyColtyp => this.key.Coltyp;

        /// <summary>
        /// Gets the JET_coltyp that the value column should have.
        /// </summary>
        public JET_coltyp ValueColtyp => this.value.Coltyp;
    }
}