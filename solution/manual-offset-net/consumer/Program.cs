using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace consumer_net {
    //----------------------------------------------------------------
    // Make sure the producer is running before you start this consumer.
    // - Upon first run the consumer starts at the beginning of the topic
    // - Interrup the consumer with CTRL-c 
    // - and the restart the consumer and observe that it continues with
    //   the next message.
    //----------------------------------------------------------------
    class Program {
        static void Main (string[] args) {
            Console.WriteLine ("Starting Consumer!");

            const string OFFSET_FILE = "topic-offset.txt";
            string topicName = "sample-topic-net";
            var conf = new Dictionary<string, object> { 
                  { "group.id", "manual-offset-group" },
                  { "bootstrap.servers", "kafka:9092" },
                  { "auto.offset.reset", "earliest" }
                };

            using (var consumer = new Consumer<string, string> (conf, new StringDeserializer (Encoding.UTF8), new StringDeserializer (Encoding.UTF8))) {
                consumer.OnMessage += (_, msg) => {
                    Console.WriteLine ($"Read ('{msg.Key}', '{msg.Value}') from: {msg.TopicPartitionOffset}");
                    File.WriteAllText(OFFSET_FILE, msg.TopicPartitionOffset.Offset.ToString());
                };

                consumer.OnError += (_, error) => 
                    Console.WriteLine ($"Error: {error}");

                consumer.OnConsumeError += (_, msg) => 
                    Console.WriteLine ($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");

                consumer.OnPartitionsAssigned += (obj, partitions) => {
                    var offset = 0;
                    if(File.Exists(OFFSET_FILE)){
                        offset = int.Parse(File.ReadAllText(OFFSET_FILE));
                        offset++;   // avoid duplicate handling of last read record
                    }
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}], member id: {consumer.MemberId}");
                    var offsets = new []{
                        new TopicPartitionOffset(topicName, 0, offset)
                    }.ToList();
                    Console.WriteLine($"Updated assignment: [{string.Join(", ", offsets)}]");
                    consumer.Assign(offsets);
                };

                consumer.Subscribe (topicName);

                while (true) {
                    consumer.Poll (TimeSpan.FromMilliseconds (100));
                }
            }
        }
    }
}