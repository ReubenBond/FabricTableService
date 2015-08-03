using System.Fabric;
using System.ServiceModel;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services;
using Microsoft.ServiceFabric.Services.Wcf;
using TableStore.Interface;

namespace TableStore.Client
{
    using System;
    using System.Linq;
    using System.Text;

    internal class Program
    {
        private static void Main(string[] args)
        {
            Run().Wait();

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        private static async Task Run()
        {
            var serviceResolver = new ServicePartitionResolver(() => new FabricClient());

            var clientFactory = new WcfCommunicationClientFactory<ITableStoreService>(
                serviceResolver,// ServicePartitionResolver
                ServiceBindings.TcpBinding,        // Client binding
                null,           // Callback object
                null);          // donot retry Exception types

            
            var client = new ServicePartitionClient<WcfCommunicationClient<ITableStoreService>>(
                clientFactory,
                new Uri("fabric:/FabricTableServiceApplication/FabricTableService"), partitionKey: 0);

            var iteration = 0;
            var value = (long)0;
            while (true)
            {
                var expected = value + 1;
                await Set(client, "a", expected);
                value = await Get(client, "a");
                if (value != expected)
                {
                    Console.WriteLine($"[{DateTime.Now}] Expected {expected} but encountered {value}. Iteration {iteration++}.");
                }

                if (iteration%100 == 0)
                {
                    Console.Write('.');
                }
            }
        }

        private static async Task<long> Get(ServicePartitionClient<WcfCommunicationClient<ITableStoreService>> client, string key)
        {
            var value = await client.InvokeWithRetryAsync(_ => _.Channel.Get(key, null));
            return BitConverter.ToInt64(value, 0);
        }

        private static Task Set(ServicePartitionClient<WcfCommunicationClient<ITableStoreService>> client, string key, long value)
        {
            return client.InvokeWithRetryAsync(_ => _.Channel.Insert(key, null, BitConverter.GetBytes(value)));
        }
    }
}