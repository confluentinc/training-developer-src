using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace app
{
    class Program
    {
        private static String INPUT_PATH_NAME = "/datasets/shakespeare";
        
        static void Main(string[] args)
        {
            Console.WriteLine(">>> Starting Shakespeare Producer");

            string brokerList = "kafka:9092";
            string topicName = "shakespeare_topic_net";

            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };

            using (var producer = new Producer<string, string>(config, 
                    new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                var files = Directory.GetFiles(INPUT_PATH_NAME);
                foreach(var file in files){
                    var key = Path.GetFileNameWithoutExtension(file);
                    Console.WriteLine($"Working on file: {key}");
                    var fs = new FileStream(file, FileMode.Open, FileAccess.Read);
                    using(var reader = new StreamReader(fs)) {
                        string line;
                        while((line = reader.ReadLine()) != null) {
                            producer.ProduceAsync(topicName, key, line);
                        }
                    }
                }
                producer.Flush(-1); // block until done
            }
            Console.WriteLine("<<< Ending Shakespeare Producer");
        }
    }
}
