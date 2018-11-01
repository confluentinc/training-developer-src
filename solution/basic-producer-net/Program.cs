using System;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace app
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(">>> Starting Basic Producer");

            string brokerList = "kafka:9092";
            string topicName = "hello-world-topic";

            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };

            using (var producer = new Producer<string, string>(config, 
                    new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                for(var i=1; i<=5; i++){
                    var key = "key-" + i.ToString();
                    var value = "value-" + i.ToString();
                    producer.ProduceAsync(topicName, key, value);
                }
                Console.WriteLine("flushing...");
                producer.Flush(-1); // block until done
            }
            Console.WriteLine("<<< Ending Basic Producer");
        }
    }
}
