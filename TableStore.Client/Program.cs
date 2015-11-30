using System.Fabric;
using System.ServiceModel;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services;
using Microsoft.ServiceFabric.Services.Wcf;
using TableStore.Interface;

namespace TableStore.Client
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
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

            var tasks = new Task[15];
            var iteration = (long)0;

            // Initialize.
            for (var i = 0; i < tasks.Length; ++i)
            {
                await Set(client, i.ToString(), iteration);
            }

            var timer = Stopwatch.StartNew();
            while (true)
            {
                for (var i = 0; i < tasks.Length; ++i)
                {
                    tasks[i] = CheckAndIncrement(client, i.ToString(), iteration);
                }

                await Task.WhenAll(tasks);

                var total = iteration * tasks.Length;
                if (iteration%100 == 0)
                {
                    //Console.Write('.');
                    Console.WriteLine($"{iteration} iterations in {timer.ElapsedMilliseconds}ms. {total *1000/ ( timer.ElapsedMilliseconds)}/sec");
                }

                if (iteration % 8000 == 0 && timer.ElapsedMilliseconds > 0)
                {
                    Console.WriteLine();
                    Console.WriteLine($"{iteration} iterations in {timer.ElapsedMilliseconds}ms. {total*1000/(timer.ElapsedMilliseconds)}/sec");
                }

                iteration++;
            }
        }

        private static async Task CheckAndIncrement(ServicePartitionClient<WcfCommunicationClient<ITableStoreService>> client, string key, long expected)
        {
            var serialized = await client.InvokeWithRetryAsync(_ => _.Channel.Get(key, null));
            var value = BitConverter.ToInt64(serialized, 0);

            if (value != expected)
            {
                throw new Exception($"[{DateTime.Now}] Expected {expected} but encountered {value}.");
            }

            await client.InvokeWithRetryAsync(_ => _.Channel.Insert(key, null, BitConverter.GetBytes(value + 1)));
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