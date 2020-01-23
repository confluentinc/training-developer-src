namespace DotnetConsumer
{
    using System;
    using System.Threading;
    using Confluent.Kafka;

    /// <summary>
    /// Dotnet consumer.
    /// </summary>
    public class Program
    {
        private const string KafkaTopic = "driver-positions";

        /// <summary>
        /// Main method for console app.
        /// </summary>
        /// <param name="args">No arguments used.</param>
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting .net consumer.");

            // Configure the group id, location of the bootstrap server, default deserializers,
            // Confluent interceptors
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "kafka:9092",
                GroupId = "csharp-consumer",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                PluginLibraryPaths = "monitoring-interceptor",
            };

            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                // Subscribe to our topic
                consumer.Subscribe(KafkaTopic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            // TODO: Consume available records
                            var cr = consumer.Consume(cts.Token);
                            // TODO: print the contents of the record
                            Console.WriteLine($"Key:{cr.Key} Value:{cr.Value} [partition {cr.Partition.Value}]");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Clean up when the application exits
                    Console.WriteLine("Closing consumer.");
                    consumer.Close();
                }
            }
        }
    }
}
