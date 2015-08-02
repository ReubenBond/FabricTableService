namespace FabricTableService.Json
{
    using System;

    using Newtonsoft.Json;

    /// <summary>
    ///     JSON converter for <see cref="Guid"/>.
    /// </summary>
    public class GuidJsonConverter : JsonConverter
    {
        /// <summary>
        ///     Gets the instance.
        /// </summary>
        public static GuidJsonConverter Instance { get; } = new GuidJsonConverter();

        public override bool CanRead => true;
        
        public override bool CanWrite => true;
        
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Guid) || objectType == typeof(Guid?);
        }

        /// <summary>
        /// Writes the JSON representation of the object.
        /// </summary>
        /// <param name="writer">
        /// The <see cref="T:Newtonsoft.Json.JsonWriter"/> to write to.
        /// </param>
        /// <param name="value">
        /// The value.
        /// </param>
        /// <param name="serializer">
        /// The calling serializer.
        /// </param>
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteValue(default(string));
            }
            else if (value is Guid)
            {
                var guid = (Guid)value;
                writer.WriteValue(guid.ToString("N"));
            }
        }

        /// <summary>
        /// Reads the JSON representation of the object.
        /// </summary>
        /// <param name="reader">
        /// The <see cref="T:Newtonsoft.Json.JsonReader"/> to read from.
        /// </param>
        /// <param name="objectType">
        /// Type of the object.
        /// </param>
        /// <param name="existingValue">
        /// The existing value of object being read.
        /// </param>
        /// <param name="serializer">
        /// The calling serializer.
        /// </param>
        /// <returns>
        /// The object value.
        /// </returns>
        public override object ReadJson(
            JsonReader reader, 
            Type objectType, 
            object existingValue, 
            JsonSerializer serializer)
        {
            var str = reader.Value as string;
            return str != null ? Guid.Parse(str) : default(Guid);
        }
    }
}