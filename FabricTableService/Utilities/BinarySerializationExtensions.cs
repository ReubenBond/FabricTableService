// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   The binary serialization extensions.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace FabricTableService.Utilities
{
    using System;
    using System.IO;
    using System.Runtime.Serialization.Formatters.Binary;
    
    /// <summary>
    /// The binary serialization extensions.
    /// </summary>
    public static class BinarySerializationExtensions
    {
        /// <summary>
        /// Writes the specified value to the <paramref name="writer"/>.
        /// </summary>
        /// <typeparam name="T">The value type.</typeparam>
        /// <param name="writer">The writer.</param>
        /// <param name="value">The value to write.</param>
        public static void WriteObject<T>(this BinaryWriter writer, T value)
        {
            var boxed = (object)value;
            switch (Type.GetTypeCode(typeof(T)))
            {
                default:
                    using (var ms = MemoryStreamManager.Pool.GetStream("WriteObject"))
                    {
                        var bf = new BinaryFormatter();
                        bf.Serialize(ms, boxed);

                        var bytes = ms.ToArray();
                        writer.Write(bytes.Length);
                        writer.Write(bytes);
                    }

                    break;
                case TypeCode.Boolean:
                    writer.Write((bool)boxed);
                    break;
                case TypeCode.Char:
                    writer.Write((char)boxed);
                    break;
                case TypeCode.SByte:
                    writer.Write((sbyte)boxed);
                    break;
                case TypeCode.Byte:
                    writer.Write((byte)boxed);
                    break;
                case TypeCode.Int16:
                    writer.Write((short)boxed);
                    break;
                case TypeCode.UInt16:
                    writer.Write((ushort)boxed);
                    break;
                case TypeCode.Int32:
                    writer.Write((int)boxed);
                    break;
                case TypeCode.UInt32:
                    writer.Write((uint)boxed);
                    break;
                case TypeCode.Int64:
                    writer.Write((long)boxed);
                    break;
                case TypeCode.UInt64:
                    writer.Write((ulong)boxed);
                    break;
                case TypeCode.Single:
                    writer.Write((float)boxed);
                    break;
                case TypeCode.Double:
                    writer.Write((double)boxed);
                    break;
                case TypeCode.Decimal:
                    writer.Write((decimal)boxed);
                    break;
                case TypeCode.DateTime:
                    writer.Write(((DateTime)boxed).Ticks);
                    break;
                case TypeCode.String:
                    writer.Write((string)boxed);
                    break;
            }
        }

        /// <summary>
        /// Reads an object from the <paramref name="reader"/>.
        /// </summary>
        /// <typeparam name="T">The type to read.</typeparam>
        /// <param name="reader">The reader.</param>
        /// <returns>The object which was read.</returns>
        public static T ReadObject<T>(this BinaryReader reader)
        {
            object result;
            switch (Type.GetTypeCode(typeof(T)))
            {
                default:
                    var length = reader.ReadInt32();
                    var bytes = reader.ReadBytes(length);
                    using (var ms = MemoryStreamManager.Pool.GetStream("WriteObject", bytes, 0, bytes.Length))
                    {
                        var bf = new BinaryFormatter();
                        result = bf.Deserialize(ms);
                    }

                    break;
                case TypeCode.Boolean:
                    result = reader.ReadBoolean();
                    break;
                case TypeCode.Char:
                    result = reader.ReadChar();
                    break;
                case TypeCode.SByte:
                    result = reader.ReadSByte();
                    break;
                case TypeCode.Byte:
                    result = reader.ReadByte();
                    break;
                case TypeCode.Int16:
                    result = reader.ReadInt16();
                    break;
                case TypeCode.UInt16:
                    result = reader.ReadUInt16();
                    break;
                case TypeCode.Int32:
                    result = reader.ReadInt32();
                    break;
                case TypeCode.UInt32:
                    result = reader.ReadUInt32();
                    break;
                case TypeCode.Int64:
                    result = reader.ReadInt64();
                    break;
                case TypeCode.UInt64:
                    result = reader.ReadUInt64();
                    break;
                case TypeCode.Single:
                    result = reader.ReadSingle();
                    break;
                case TypeCode.Double:
                    result = reader.ReadDouble();
                    break;
                case TypeCode.Decimal:
                    result = reader.ReadDecimal();
                    break;
                case TypeCode.DateTime:
                    result = new DateTime(reader.ReadInt64());
                    break;
                case TypeCode.String:
                    result = reader.ReadString();
                    break;
            }

            return (T)result;
        }
    }
}
