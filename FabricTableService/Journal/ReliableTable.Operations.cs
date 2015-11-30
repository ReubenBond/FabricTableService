// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DistributedJournal.Operations.cs" company="">
//   
// </copyright>
// <summary>
//   The distributed journal.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace FabricTableService.Journal
{
    using System;
    using System.Collections.Generic;
    using System.IO;

    using global::FabricTableService.Journal.Database;
    using global::FabricTableService.Utilities;

    using Microsoft.ServiceFabric.Data;

    /// <summary>
    /// A distributed journal.
    /// </summary>
    /// <typeparam name="TKey">
    /// The key type.
    /// </typeparam>
    /// <typeparam name="TValue">
    /// The value type.
    /// </typeparam>
    public partial class ReliableTable<TKey, TValue>
    {
        /// <summary>
        /// The operation type id, used to uniquely identify an operation.
        /// </summary>
        public enum OperationType : ushort
        {
            /// <summary>
            /// The none.
            /// </summary>
            None,

            /// <summary>
            /// The set.
            /// </summary>
            Set = 1,

            /// <summary>
            /// Removes an item.
            /// </summary>
            Remove,

            /// <summary>
            /// Gets an item.
            /// </summary>
            Get,

            /// <summary>
            /// Null operation.
            /// </summary>
            Nop
        }

        /// <summary>
        /// The operation.
        /// </summary>
        internal abstract class Operation
        {
            /// <summary>
            /// The constructors.
            /// </summary>
            // ReSharper disable once StaticMemberInGenericType
            protected static readonly Dictionary<OperationType, Func<Operation>> Constructors =
                new Dictionary<OperationType, Func<Operation>>
                {
                    { OperationType.Set, () => new SetOperation() },
                    { OperationType.Remove, () => new RemoveOperation() },
                    { OperationType.Get, () => new GetOperation() },
                    { OperationType.Nop, () => new NopOperation() }
                };

            /// <summary>
            /// The operation types.
            /// </summary>
            // ReSharper disable once StaticMemberInGenericType
            protected static readonly Dictionary<Type, OperationType> OperationTypes = new Dictionary<Type, OperationType>();

            /// <summary>
            /// Initializes static members of the <see cref="Operation"/> class.
            /// </summary>
            static Operation()
            {
                // Create the mapping between types and operations.
                foreach (var constructor in Constructors)
                {
                    var type = constructor.Value().GetType();
                    OperationTypes[type] = constructor.Key;
                }
            }

            /// <summary>
            /// Gets the type of this operation.
            /// </summary>
            public OperationType Type => OperationTypes[this.GetType()];

            /// <summary>
            /// Gets or sets the id.
            /// </summary>
            public long Id { get; set; }

            /// <summary>
            /// Gets or sets the version.
            /// </summary>
            public long Version { get; set; }

            /// <summary>
            /// The deserialize.
            /// </summary>
            /// <param name="bytes">
            /// The bytes.
            /// </param>
            /// <returns>
            /// The <see cref="Operation"/>.
            /// </returns>
            /// <exception cref="ApplicationException">
            /// The operation type was unknown.
            /// </exception>
            public static Operation Deserialize(byte[] bytes)
            {
                try
                {
                    using (var mem = MemoryStreamManager.Pool.GetStream("Deseralize", bytes, 0, bytes.Length))
                    using (var reader = new BinaryReader(mem))
                    {
                        var type = (OperationType)reader.ReadUInt16();
                        Func<Operation> constructor;

                        if (!Constructors.TryGetValue(type, out constructor))
                        {
                            throw new NotSupportedException("Unknown operation type " + type);
                        }

                        var result = constructor();
                        result.Version = reader.ReadInt64();
                        result.Id = reader.ReadInt64();
                        result.DeserializeInternal(reader);
                        return result;
                    }
                }
                catch
                {
                    return null;
                }
            }

            /// <summary>
            /// Serializes this operation.
            /// </summary>
            /// <returns>The serialized operation.</returns>
            public byte[] Serialize()
            {
                using (var mem = MemoryStreamManager.Pool.GetStream("Serialize"))
                using (var writer = new BinaryWriter(mem))
                {
                    writer.Write((ushort)this.Type);
                    writer.Write(this.Version);
                    writer.Write(this.Id);
                    this.SerializeInternal(writer);
                    return mem.ToArray();
                }
            }

            /// <summary>
            /// Applies the operation to the table.
            /// </summary>
            /// <param name="table">The table.</param>
            /// <returns>The result of applying the operation.</returns>
            public abstract object Apply(PersistentTable<TKey, TValue> table);

            /// <summary>
            /// Deserializes operation-specific fields.
            /// </summary>
            /// <param name="reader">The reader.</param>
            protected abstract void DeserializeInternal(BinaryReader reader);

            /// <summary>
            /// Serializes operation-specific fields.
            /// </summary>
            /// <param name="writer">The writer.</param>
            protected abstract void SerializeInternal(BinaryWriter writer);
        }

        /// <summary>
        /// The set operation.
        /// </summary>
        internal class SetOperation : Operation
        {
            /// <summary>
            /// Gets or sets the value.
            /// </summary>
            public TValue Value { get; set; }

            /// <summary>
            /// Gets or sets the key.
            /// </summary>
            public TKey Key { get; set; }

            /// <summary>
            /// Applies the operation to the table.
            /// </summary>
            /// <param name="table">The table.</param>
            public override object Apply(PersistentTable<TKey, TValue> table)
            {
                table.AddOrUpdate(this.Key, this.Value);
                return null;
            }

            /// <summary>
            /// Deserializes operation-specific fields.
            /// </summary>
            /// <param name="reader">The reader.</param>
            protected override void DeserializeInternal(BinaryReader reader)
            {
                this.Key = reader.ReadObject<TKey>();
                this.Value = reader.ReadObject<TValue>();
            }

            /// <summary>
            /// Serializes operation-specific fields.
            /// </summary>
            /// <param name="writer">The writer.</param>
            protected override void SerializeInternal(BinaryWriter writer)
            {
                writer.WriteObject(this.Key);
                writer.WriteObject(this.Value);
            }
        }

        /// <summary>
        /// Represents a remove operation.
        /// </summary>
        internal class RemoveOperation : Operation
        {
            /// <summary>
            /// Gets or sets the key.
            /// </summary>
            public TKey Key { get; set; }

            /// <summary>
            /// Applies the operation to the journal.
            /// </summary>
            /// <param name="table">The journal.</param>
            public override object Apply(PersistentTable<TKey, TValue> table)
            {
                TValue value;
                return table.TryRemove(this.Key, out value);
            }

            /// <summary>
            /// Deserializes operation-specific fields.
            /// </summary>
            /// <param name="reader">The reader.</param>
            protected override void DeserializeInternal(BinaryReader reader)
            {
                this.Key = reader.ReadObject<TKey>();
            }

            /// <summary>
            /// Serializes operation-specific fields.
            /// </summary>
            /// <param name="writer">The writer.</param>
            protected override void SerializeInternal(BinaryWriter writer)
            {
                writer.WriteObject(this.Key);
            }
        }

        /// <summary>
        /// Represents a get operation.
        /// </summary>
        internal class GetOperation : Operation
        {
            /// <summary>
            /// Gets or sets the key.
            /// </summary>
            public TKey Key { get; set; }

            /// <summary>
            /// Applies the operation to the journal.
            /// </summary>
            /// <param name="table">The journal.</param>
            public override object Apply(PersistentTable<TKey, TValue> table)
            {
                TValue value;
                var result = table.TryGetValue(this.Key, out value);
                return new ConditionalResult<TValue>(result, value);
            }

            /// <summary>
            /// Deserializes operation-specific fields.
            /// </summary>
            /// <param name="reader">The reader.</param>
            protected override void DeserializeInternal(BinaryReader reader)
            {
                this.Key = reader.ReadObject<TKey>();
            }

            /// <summary>
            /// Serializes operation-specific fields.
            /// </summary>
            /// <param name="writer">The writer.</param>
            protected override void SerializeInternal(BinaryWriter writer)
            {
                writer.WriteObject(this.Key);
            }
        }

        /// <summary>
        /// Represents a null operation.
        /// </summary>
        internal class NopOperation : Operation
        {
            /// <summary>
            /// A null operation.
            /// </summary>
            // ReSharper disable once StaticMemberInGenericType
            public static readonly NopOperation Instance = new NopOperation();

            /// <summary>
            /// Applies the operation to the journal.
            /// </summary>
            /// <param name="table">The journal.</param>
            public override object Apply(PersistentTable<TKey, TValue> table)
            {
                return null;
            }

            /// <summary>
            /// Deserializes operation-specific fields.
            /// </summary>
            /// <param name="reader">The reader.</param>
            protected override void DeserializeInternal(BinaryReader reader)
            {
            }

            /// <summary>
            /// Serializes operation-specific fields.
            /// </summary>
            /// <param name="writer">The writer.</param>
            protected override void SerializeInternal(BinaryWriter writer)
            {
            }
        }
    }
}
