namespace DotnetConsumer
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
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
                GroupId = "csharp-consumer-prev",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                PluginLibraryPaths = "monitoring-interceptor",
            };

            using (var consumer = new ConsumerBuilder<string, string>(consumerConfig)
            .SetPartitionsAssignedHandler((c, partitions) =>
            {
                // Calculate the time 5 minutes ago
                var timestamp = new Confluent.Kafka.Timestamp(DateTime.Now.AddMinutes(-5));
                // Build a list of TopicPartitionTimestamp
                var timestamps = partitions.Select(tp => new TopicPartitionTimestamp(tp, timestamp));
                // TODO: Request the offsets for the start timestamp
                var offsets = c.OffsetsForTimes(???
                foreach (var offset in offsets)
                {
                    // TODO: Print the new offset for each partition
                    Console.WriteLine(???
                }

                // Return the new partition offsets
                return offsets;
            })
            .Build())
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
                    int recordCount = 0;
                    while (true)
                    {
                        try
                        {
                            // Poll for available records
                            var cr = consumer.Consume(cts.Token);
                            Console.WriteLine($"{cr.Key},{cr.Value}");
                            recordCount++;
                            // Exit processing after 100 records
                            if (recordCount >= 100)
                            {
                                break;
                            }
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
