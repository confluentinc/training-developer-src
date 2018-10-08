using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace app
{
    public class Program {
        private static string bootstrapServers = "kafka-9092";
        private static string schemaRegistryUrl = "schema-registry:8081";
        private static string topicName = "shakespeare_avro_topic_net";

        static void Main(string[] args){
            Console.WriteLine(">>> Starting Shakespeare Avro Consumer");

            Console.CancelKeyPress += delegate {
                Console.WriteLine("<<< Ending Shakespeare Avro Consumer\n");
            };

            var consumerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "group.id", Guid.NewGuid() },
                { "schema.registry.url", schemaRegistryUrl },
                { "auto.offset.reset", "beginning" }
            };
            using (var consumer = new Consumer<ShakespeareKey, ShakespeareValue>(
                consumerConfig, 
                new AvroDeserializer<ShakespeareKey>(), 
                new AvroDeserializer<ShakespeareValue>())) {

                consumer.OnMessage += (_, msg) =>
                {
                    Console.WriteLine($"Work: {msg.Key.work}, Year: {msg.Key.year}, line: {msg.Value.line}");
                };

                consumer.OnError += (_, e)
                    => Console.WriteLine("Error: " + e.Reason);

                consumer.OnConsumeError += (_, e)
                    => Console.WriteLine("Consume error: " + e.Error.Reason);

                consumer.Subscribe(topicName);
                while (true)
                {
                    consumer.Poll(100);
                }
            }
        }
    }
}