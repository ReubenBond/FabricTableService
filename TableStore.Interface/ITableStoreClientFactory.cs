namespace TableStore.Interface
{
    using System.Threading.Tasks;

    public interface ITableStoreClientFactory
    {
        Task<ITableStoreClient> CreateClient(string endpoint);
    }
}