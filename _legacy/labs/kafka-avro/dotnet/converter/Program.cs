using System;
using System.Text;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace app
{
    class Program
    {
        private static string bootstrapServers = "kafka:9092";
        private static string schemaRegistryUrl = "schema-registry:8081";
        private static string inputTopicName = "shakespeare_topic_net";
        private static string outputTopicName = "shakespeare_avro_topic_net";
        private static Dictionary<string,int> shakespeareWorkToYearWritten;
        private static Regex regex = new Regex("^\\s*(\\d*)\\s*(.*)$", RegexOptions.IgnoreCase);

        private Consumer<string,string> CreateConsumer() {
          var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "group.id", "shakespeare-consumer" },
                { "auto.offset.reset", "earliest" }
            };

            var consumer = new Consumer<string, string>(config, 
                new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8));
            return consumer;
        }

        private Producer<ShakespeareKey, ShakespeareValue> CreateProducer(){
            var producerConfig = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "schema.registry.url", schemaRegistryUrl }
            };
            var producer = new Producer<ShakespeareKey, ShakespeareValue>(producerConfig, 
                new AvroSerializer<ShakespeareKey>(), 
                new AvroSerializer<ShakespeareValue>());
            return producer;
        }

        private ShakespeareKey GetShakespeareKey(string key){
            var ok = shakespeareWorkToYearWritten.TryGetValue(key, out int yearWritten);
            if (!ok) {
                throw new Exception($"Could not find year written for '{key}'");
            }
            return new ShakespeareKey{work=key, year=yearWritten};
        }

        private ShakespeareValue GetShakespeareValue(string line){
            Match m = regex.Match(line);
            // Use a regex to parse out the line number from the rest of the line
            if (m.Success) {
                // Get the line number and line and create the ShakespeareLine
                var lineNumber = int.Parse(m.Groups[1].Value);
                var lineOfWork = m.Groups[2].Value;
                return new ShakespeareValue{line_number=lineNumber, line=lineOfWork};
            } else {
                // Line didn't match the regex
                Console.WriteLine("Did not match Regex:" + line);
                return null;
            }
        }

        public void Run(){
            var producer = CreateProducer();

            Console.CancelKeyPress += delegate {
                Console.WriteLine("### Ending: waiting until producer has flushed all records...");
                producer.Flush(-1); // block until done
                producer.Dispose();
                Console.WriteLine("<<< Ending Shakespeare Converter\n");
            };

            using(var consumer = CreateConsumer()){
                consumer.OnMessage += (_, msg) =>
                    {
                        Console.WriteLine($"Topic: {msg.Topic} Partition: {msg.Partition} Offset: {msg.Offset} {msg.Value}");
                        var key = GetShakespeareKey(msg.Key);
                        var value = GetShakespeareValue(msg.Value);
                        producer.ProduceAsync(outputTopicName, key, value);
                    };

                consumer.Subscribe(inputTopicName);
                while (true)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                }
            }
        }

        static void Main(string[] args)
        {
            Console.WriteLine(">>> Starting Shakespeare Converter");

            shakespeareWorkToYearWritten = new Dictionary<string,int>();
            shakespeareWorkToYearWritten.Add("Hamlet", 1600);
            shakespeareWorkToYearWritten.Add("Julius Caesar", 1599);
            shakespeareWorkToYearWritten.Add("Macbeth", 1605);
            shakespeareWorkToYearWritten.Add("Merchant of Venice", 1596);
            shakespeareWorkToYearWritten.Add("Othello", 1604);
            shakespeareWorkToYearWritten.Add("Romeo and Juliet", 1594);

            var converter = new Program();
            converter.Run();
        }
    }
}
