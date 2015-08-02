namespace FabricTableService.Json
{
    using Newtonsoft.Json;

    /// <summary>
    /// JSON serialization settings.
    /// </summary>
    public static class SerializationSettings
    {
        /// <summary>
        ///     Initializes static members of the <see cref="SerializationSettings"/> class.
        /// </summary>
        static SerializationSettings()
        {
            JsonConfig = new JsonSerializerSettings
                                     {
                                         ContractResolver = JsonContractResolver.Instance, 
                                         NullValueHandling = NullValueHandling.Include, 
                                         MissingMemberHandling = MissingMemberHandling.Ignore, 
                                         DefaultValueHandling = DefaultValueHandling.Populate, 
                                         CheckAdditionalContent = false
                                     };

            JsonSerializer = JsonSerializer.Create(JsonConfig);

            // Set the default JSON serializer.
            JsonConvert.DefaultSettings = () => JsonConfig;
        }

        /// <summary>
        ///     Gets the JSON serializer settings.
        /// </summary>
        public static JsonSerializerSettings JsonConfig { get; }

        /// <summary>
        ///     Gets the JSON serializer.
        /// </summary>
        public static JsonSerializer JsonSerializer { get; private set; }
    }
}