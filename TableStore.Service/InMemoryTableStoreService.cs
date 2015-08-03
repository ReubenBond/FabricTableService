namespace TableStore.Service
{
    using System;
    using System.Collections.Concurrent;
    using System.ServiceModel;
    using System.Threading.Tasks;
    using Interface;

    [ServiceBehavior(
        InstanceContextMode = InstanceContextMode.Single,
        ConcurrencyMode = ConcurrencyMode.Multiple,
        AddressFilterMode = AddressFilterMode.Any,
        IncludeExceptionDetailInFaults = true)]
    public class InMemoryTableStoreService : ITableStoreService
    {

        private readonly ConcurrentDictionary<Tuple<string, string>, byte[]> table =
            new ConcurrentDictionary<Tuple<string, string>, byte[]>();

        public Task Insert(string key, string partition, byte[] value)
        {
            this.table.AddOrUpdate(Tuple.Create(partition, key), value, (_, __) => value);
            return Task.FromResult(0);
        }

        public Task<byte[]> Get(string key, string partition)
        {
            byte[] value;
            this.table.TryGetValue(Tuple.Create(partition, key), out value);
            return Task.FromResult(value);
        }

        public Task Delete(string key, string partition)
        {
            byte[] value;
            this.table.TryRemove(Tuple.Create(partition, key), out value);
            return Task.FromResult(0);
        }
    }
}
