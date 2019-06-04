using System;
using System.IO;
using System.Linq;
using System.Threading;
using Confluent.Kafka;

namespace app {
    class Program {
        static string OFFSET_FILE_PREFIX = "./offsets/offset_";
        static void Main (string[] args) {
            Console.WriteLine ("Starting .NET Consumer!");
            var conf = new ConsumerConfig { 
                    GroupId = "vp-consumer-group-net",
                    BootstrapServers = "kafka:9092",
                    AutoCommitIntervalMs = 5000,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };

            using (var consumer = new ConsumerBuilder<string, string> (conf)
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    Console.WriteLine("Resetting all partitions to offset found in file.");
                    return partitions.Select(tp => {
                        var path = Path.GetFullPath(OFFSET_FILE_PREFIX + tp.Partition.Value);
                        long offset = tp.Partition.Value;
                        if(File.Exists(path)){
                            offset = long.Parse(File.ReadAllText(path));
                        }
                        return new TopicPartitionOffset(tp, new Offset(offset));
                    });
                })
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
                            File.WriteAllText(Path.GetFullPath(
                                OFFSET_FILE_PREFIX + cr.TopicPartition.Partition.Value), 
                                cr.TopicPartitionOffset.Offset.Value.ToString());
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