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
                  { "group.id", "dotnet-basic-consumer-group" },
                  { "bootstrap.servers", "kafka:9092" },
                  { "enable.auto.commit", true },
                  { "auto.commit.interval.ms", 1000 },
                  { "auto.offset.reset", "earliest" }
                };

            using (var consumer = new Consumer<string, string> (conf, new StringDeserializer (Encoding.UTF8), new StringDeserializer (Encoding.UTF8))) {
                consumer.OnMessage += (_, msg) => 
                  Console.WriteLine ($"Read ('{msg.Key}', '{msg.Value}') from: {msg.TopicPartitionOffset}");

                consumer.OnError += (_, error) => 
                  Console.WriteLine ($"Error: {error}");

                consumer.OnConsumeError += (_, msg) => 
                  Console.WriteLine ($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");

                consumer.Subscribe ("hello-world-topic");

                while (true) {
                    consumer.Poll (TimeSpan.FromMilliseconds (100));
                }
            }
        }
    }
}