namespace TableStore.Interface
{
    using System.Threading.Tasks;

    public interface ITableStoreClient
    {
        Task Insert(string key, string partition, byte[] value);
        Task<byte[]> Get(string key, string partition);
        Task Delete(string key, string partition);
    }
}