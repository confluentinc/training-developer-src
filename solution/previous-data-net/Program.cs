using System;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace consumer_net {
    class Program {
        static void Main (string[] args) {
            Console.WriteLine ("Starting Consumer!");
            var conf = new Dictionary<string, object> { 
                  { "group.id", "dotnet-consumer-group" },
                  { "bootstrap.servers", "kafka-9092" },
                  { "auto.commit.interval.ms", 5000 },
                  { "auto.offset.reset", "earliest" }
                };

            using (var consumer = new Consumer<string, string> (conf, new StringDeserializer (Encoding.UTF8), new StringDeserializer (Encoding.UTF8))) {
                consumer.OnMessage += (_, msg) => 
                  Console.WriteLine ($"Read ('{msg.Key}', '{msg.Value}') from: {msg.TopicPartitionOffset}");

                consumer.OnError += (_, error) => 
                  Console.WriteLine ($"Error: {error}");

                consumer.OnConsumeError += (_, msg) => 
                  Console.WriteLine ($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");

                consumer.OnPartitionsAssigned += (obj, partitions) => {
                  // Always resetting the offset to the beginning (for all partitions!)
                  Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                  var fromBeginning = partitions.Select(p => new TopicPartitionOffset(p.Topic, p.Partition, Offset.Beginning)).ToList();
                  Console.WriteLine($"Updated assignment: [{string.Join(", ", fromBeginning)}]");
                  consumer.Assign(fromBeginning);
                };

                consumer.Subscribe ("hello-world-topic");

                while (true) {
                    consumer.Poll (TimeSpan.FromMilliseconds (100));
                }
            }
        }
    }
}