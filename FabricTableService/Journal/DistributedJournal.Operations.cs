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
    using System.Threading;

    using Microsoft.Isam.Esent.Interop;

    /// <summary>
    /// The distributed journal.
    /// </summary>
    public partial class DistributedJournal<TKey, TValue>
    {
        /// <summary>
        /// The operation.
        /// </summary>
        public abstract class Operation
        {
			/// <summary>
			/// The sequence number counter.
			/// </summary>
            private static int sequence;

            /// <summary>
            /// The constructors.
            /// </summary>
            protected static readonly Dictionary<OperationType, Func<Operation>> constructors =
                new Dictionary<OperationType, Func<Operation>>
                {
                    { OperationType.Set, () => new SetOperation() },
                    { OperationType.Remove, () => new RemoveOperation() }
                };

            /// <summary>
            /// The constructors.
            /// </summary>
            protected static readonly Dictionary<Type, OperationType> operationTypes = new Dictionary<Type, OperationType>();

            static Operation()
            {
                foreach (var constructor in constructors)
                {
                    var type = constructor.Value().GetType();
                    operationTypes[type] = constructor.Key;
                }
            }

            /// <summary>
            /// Initializes a new instance of the <see cref="Operation"/> class.
            /// </summary>
            protected Operation()
            {
                this.Id = Interlocked.Increment(ref sequence);
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
                Remove
            }

			/// <summary>
			/// Gets the type of this operation.
			/// </summary>
            public OperationType Type
            {
                get
                {
                    return operationTypes[this.GetType()];
                }
            }

            /// <summary>
            /// Gets or sets the id.
            /// </summary>
            public long Id { get; set; }

            /// <summary>
            /// Gets or sets the version.
            /// </summary>
            public short Version { get; set; }

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
            /// </exception>
            public static Operation Deserialize(byte[] bytes)
            {
                try
                {
                    using (var mem = new MemoryStream(bytes))
                    using (var br = new BinaryReader(mem))
                    {
                        var id = (OperationType)br.ReadUInt16();
                        Func<Operation> constructor;

                        if (!constructors.TryGetValue(id, out constructor))
                        {
                            throw new NotSupportedException("Unknown operation type " + id);
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

            protected abstract void DeserializeInternal(BinaryReader reader);

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

            protected abstract void SerializeInternal(BinaryWriter writer);

            public abstract void Apply(DistributedJournal<TKey,TValue> journal);
        }

        /// <summary>
        /// The set operation.
        /// </summary>
        internal class SetOperation : Operation
        {
            public TValue Value { get; set; }

            public TKey Key { get; set; }

            protected override void DeserializeInternal(BinaryReader reader)
            {
                this.Version = reader.ReadInt16();
                this.Id = reader.ReadInt64();
                this.Key = reader.ReadObject<TKey>();
                this.Value = reader.ReadObject<TValue>();
            }

            protected override void SerializeInternal(BinaryWriter writer)
            {
                writer.Write(this.Version);
                writer.Write(this.Id);
                writer.WriteObject(this.Key);
                writer.WriteObject(this.Value);
            }

            public override void Apply(DistributedJournal<TKey, TValue> journal)
            {
                using (var tx = new Microsoft.Isam.Esent.Interop.Transaction(journal.table.Session))
                {
                    journal.table.AddOrUpdate(this.Key, this.Value);
                    tx.Commit(CommitTransactionGrbit.None);
                }

            }
        }

        /// <summary>
        /// The set operation.
        /// </summary>
        internal class RemoveOperation : Operation
        {
            public TKey Key { get; set; }

            protected override void DeserializeInternal(BinaryReader reader)
            {
                this.Version = reader.ReadInt16();
                this.Id = reader.ReadInt64();
                this.Key = reader.ReadObject<TKey>();
            }

            protected override void SerializeInternal(BinaryWriter writer)
            {
                writer.Write(this.Version);
                writer.Write(this.Id);
                writer.WriteObject(this.Key);
            }

            public override void Apply(DistributedJournal<TKey, TValue> journal)
            {
                using (var tx = new Microsoft.Isam.Esent.Interop.Transaction(journal.table.Session))
                {
                    TValue value;
                    journal.table.TryRemove(this.Key, out value);
                    tx.Commit(CommitTransactionGrbit.None);
                }
            }
        }
    }
}
