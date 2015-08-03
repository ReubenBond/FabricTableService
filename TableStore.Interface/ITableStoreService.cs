namespace TableStore.Interface
{
    using System.ServiceModel;
    using System.Threading.Tasks;

    [ServiceContract(Name = "ITableStoreService", Namespace = "http://dapr")]
    public interface ITableStoreService
    {
        [OperationContract]
        Task Insert(string key, string partition, byte[] value);

        [OperationContract]
        Task<byte[]> Get(string key, string partition);

        [OperationContract]
        Task Delete(string key, string partition);
    }
}
