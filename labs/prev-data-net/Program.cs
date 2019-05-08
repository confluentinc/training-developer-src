using System;
using System.Linq;
using System.Threading;
using Confluent.Kafka;

namespace app {
    class Program {
        static void Main (string[] args) {
            Console.WriteLine ("Starting .NET Consumer!");
            var conf = new ConsumerConfig { 
                    GroupId = "vp-consumer-group-net",
                    BootstrapServers = "kafka:9092",
                    AutoCommitIntervalMs = 5000,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };

            using (var consumer = new ConsumerBuilder<string, string> (conf)
                .Build()) 
            {
                consumer.Subscribe("vehicle-positions");

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    consumer.Close();
                }
            }
        }
    }
}