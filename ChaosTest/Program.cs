using System;
using System.Fabric;
using System.Fabric.Testability.Scenario;
using System.Threading;
using System.Threading.Tasks;

namespace ChaosTest
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            string clusterConnection = "localhost:19000";

            Console.WriteLine("Starting Chaos Test Scenario...");
            try
            {
                RunChaosTestScenarioAsync(clusterConnection).Wait();
            }
            catch (AggregateException ae)
            {
                Console.WriteLine("Chaos Test Scenario did not complete: ");
                foreach (Exception ex in ae.InnerExceptions)
                {
                    if (ex is FabricException)
                    {
                        Console.WriteLine("HResult: {0} Message: {1}", ex.HResult, ex.Message);
                    }
                }
            }

            Console.WriteLine("Chaos Test Scenario completed.");
        }

        static async Task RunChaosTestScenarioAsync(string clusterConnection)
        {
            TimeSpan maxClusterStabilizationTimeout = TimeSpan.FromSeconds(180);
            uint maxConcurrentFaults = 3;
            bool enableMoveReplicaFaults = true;

            // Create FabricClient with connection & security information here.
            FabricClient fabricClient = new FabricClient(clusterConnection);

            // The Chaos Test Scenario should run at least 60 minutes or up until it fails.
            TimeSpan timeToRun = TimeSpan.FromMinutes(60);
            ChaosTestScenarioParameters scenarioParameters = new ChaosTestScenarioParameters(
              maxClusterStabilizationTimeout,
              maxConcurrentFaults,
              enableMoveReplicaFaults,
              timeToRun);

            // Other related parameters:
            // Pause between two iterations for a random duration bound by this value.
            // scenarioParameters.WaitTimeBetweenIterations = TimeSpan.FromSeconds(30);
            // Pause between concurrent actions for a random duration bound by this value.
            // scenarioParameters.WaitTimeBetweenFaults = TimeSpan.FromSeconds(10);

            // Create the scenario class and execute it asynchronously.
            ChaosTestScenario chaosScenario = new ChaosTestScenario(fabricClient, scenarioParameters);

            try
            {
                await chaosScenario.ExecuteAsync(CancellationToken.None);
            }
            catch (AggregateException ae)
            {
                throw ae.InnerException;
            }
        }
    }
}
