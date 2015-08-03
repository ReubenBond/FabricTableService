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
    using System.Diagnostics;
    using System.IO;

    using global::FabricTableService.Journal.Database;
    using global::FabricTableService.Utilities;

    /// <summary>
    /// The distributed journal.
    /// </summary>
    /// <typeparam name="TKey">
    /// The key type.
    /// </typeparam>
    /// <typeparam name="TValue">
    /// The value type.
    /// </typeparam>
    public partial class DistributedJournal<TKey, TValue>
    {
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
                foreach (var constructor in Constructors)
                {
                    var type = constructor.Value().GetType();
                    OperationTypes[type] = constructor.Key;
                }
            }
            
            /// <summary>
            /// The type id.
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
                    using (var mem = new MemoryStream(bytes))
                    using (var br = new BinaryReader(mem))
                    {
                        var type = (OperationType)br.ReadUInt16();
                        Func<Operation> constructor;

                        if (!Constructors.TryGetValue(type, out constructor))
                        {
                            throw new NotSupportedException("Unknown operation type " + type);
                        }

                        var result = constructor();
                        result.DeserializeInternal(br);
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
                using (var mem = new MemoryStream())
                using (var writer = new BinaryWriter(mem))
                {
                    writer.Write((ushort)this.Type);
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
                Trace.TraceInformation($"After Set {this.Key} = {this.Value}, value is {table.Get(this.Key)}");
                return null;
            }

            /// <summary>
            /// Deserializes operation-specific fields.
            /// </summary>
            /// <param name="reader">The reader.</param>
            protected override void DeserializeInternal(BinaryReader reader)
            {
                this.Version = reader.ReadInt64();
                this.Id = reader.ReadInt64();
                this.Key = reader.ReadObject<TKey>();
                this.Value = reader.ReadObject<TValue>();
            }

            /// <summary>
            /// Serializes operation-specific fields.
            /// </summary>
            /// <param name="writer">The writer.</param>
            protected override void SerializeInternal(BinaryWriter writer)
            {
                writer.Write(this.Version);
                writer.Write(this.Id);
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
                var result = table.TryRemove(this.Key, out value);

                return result;
            }

            /// <summary>
            /// Deserializes operation-specific fields.
            /// </summary>
            /// <param name="reader">The reader.</param>
            protected override void DeserializeInternal(BinaryReader reader)
            {
                this.Version = reader.ReadInt64();
                this.Id = reader.ReadInt64();
                this.Key = reader.ReadObject<TKey>();
            }

            /// <summary>
            /// Serializes operation-specific fields.
            /// </summary>
            /// <param name="writer">The writer.</param>
            protected override void SerializeInternal(BinaryWriter writer)
            {
                writer.Write(this.Version);
                writer.Write(this.Id);
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

                return Tuple.Create(result, value);
            }

            /// <summary>
            /// Deserializes operation-specific fields.
            /// </summary>
            /// <param name="reader">The reader.</param>
            protected override void DeserializeInternal(BinaryReader reader)
            {
                this.Version = reader.ReadInt64();
                this.Id = reader.ReadInt64();
                this.Key = reader.ReadObject<TKey>();
            }

            /// <summary>
            /// Serializes operation-specific fields.
            /// </summary>
            /// <param name="writer">The writer.</param>
            protected override void SerializeInternal(BinaryWriter writer)
            {
                writer.Write(this.Version);
                writer.Write(this.Id);
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
                this.Version = reader.ReadInt64();
                this.Id = reader.ReadInt64();
            }

            /// <summary>
            /// Serializes operation-specific fields.
            /// </summary>
            /// <param name="writer">The writer.</param>
            protected override void SerializeInternal(BinaryWriter writer)
            {
                writer.Write(this.Version);
                writer.Write(this.Id);
            }
        }
    }
}
