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

    /// <summary>
    /// The distributed journal.
    /// </summary>
    public partial class DistributedJournal
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
                new Dictionary<OperationType, Func<Operation>> { { OperationType.Set, () => new SetOperation() } };

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
            public enum OperationType : uint
            {
                /// <summary>
                /// The none.
                /// </summary>
                None,

                /// <summary>
                /// The set.
                /// </summary>
                Set = 1
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
                        var id = (OperationType)br.ReadInt32();
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

            public abstract void DeserializeInternal(BinaryReader reader);

            public byte[] Serialize()
            {
                using (var mem = new MemoryStream())
                using (var writer = new BinaryWriter(mem))
                {
                    writer.Write((uint)this.Type);
					this.SerializeInternal(writer);
                    return mem.ToArray();
                }
            }

            protected abstract void SerializeInternal(BinaryWriter writer);
        }
    }

    /// <summary>
    /// The set operation.
    /// </summary>
    internal class SetOperation : DistributedJournal.Operation
    {
        public string Value { get; set; }

        public override void DeserializeInternal(BinaryReader reader)
        {
            this.Version = reader.ReadInt16();
            this.Id = reader.ReadInt64();
            this.Value = reader.ReadString();
        }

        protected override void SerializeInternal(BinaryWriter writer)
        {
            writer.Write(this.Version);
            writer.Write(this.Id);
            writer.Write(this.Value);
        }
    }
}
