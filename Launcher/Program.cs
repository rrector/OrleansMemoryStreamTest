using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace Launcher
{
    class Program
    {
        public static Guid streamGuid = Guid.NewGuid();

        public static async Task Main(string[] args)
        {
            try
            {
                var host = await StartSilo();
                var client = await CreateClient();

                await Program.ProduceEvents(client);

                await host.StopAsync();

                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return;
            }
        }

        private static async Task<ISiloHost> StartSilo()
        {
            var builder = new SiloHostBuilder()
                // Use localhost clustering for a single local silo
                .UseLocalhostClustering()
                // Configure ClusterId and ServiceId
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "MyAwesomeService";
                })
            // Configure connectivity
            .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                // Configure logging with any logging framework that supports Microsoft.Extensions.Logging.
                // In this particular case it logs using the Microsoft.Extensions.Logging.Console package.
                .ConfigureLogging(logging => logging.AddConsole())
            .AddMemoryStreams<DefaultMemoryMessageBodySerializer>("MemoryTest")
            .AddMemoryGrainStorage("PubSubStore")
            .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IReceiverGrain).Assembly).WithCodeGeneration())
            ;
            var host = builder.Build();
            await host.StartAsync();
            return host;
        }

        private static async Task<IClusterClient> CreateClient()
        {
            var client = new ClientBuilder()
                // Clustering information
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "MyAwesomeClient";
                })
                // Clustering provider
                .UseLocalhostClustering()
                .AddMemoryStreams<DefaultMemoryMessageBodySerializer>("MemoryTest")

                // Application parts: just reference one of the grain interfaces that we use
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IReceiverGrain).Assembly).WithCodeGeneration())
                .Build();

            await client.Connect();
            return client;
        }

        private static async Task ProduceEvents(IClusterClient client)
        {
            var ourSequence = 0;
            var rng = new Random();
            var streamProvider = client.GetStreamProvider("MemoryTest");
            var stream = streamProvider.GetStream<Tuple<int, int>>(streamGuid, "TESTDATA");

            while (true)
            {
                Console.WriteLine("Enter the number of events to generate, 0 to quit");
                var input = Console.ReadLine();

                if (int.TryParse(input, out var numEvents))
                {
                    if (numEvents < 1)
                    {
                        return;
                    }

                    var curEvent = 0;

                    while (curEvent < numEvents)
                    {
                        var batchSize = Math.Min(numEvents - curEvent, rng.Next(1, 5));
                        var batch = new List<Tuple<int, int>>(batchSize);

                        for (int i = 0; i < batchSize; i++, curEvent++, ourSequence++)
                        {
                            batch.Add(new Tuple<int, int>(ourSequence, i));
                        }

                        await stream.OnNextBatchAsync(batch);
                    }
                }
            }
        }
    }
}
