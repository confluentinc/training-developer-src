using System;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace app
{
    //----------------------------------------------------------------
    // Make sure this producer is running while you run the consumer. 
    // The producer produces a million messages, one every 10 ms.
    //----------------------------------------------------------------
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine(">>> Starting Manual Offset Producer");

            string brokerList = "kafka:9092";
            string topicName = "sample-topic-net";

            var config = new Dictionary<string, object> { { "bootstrap.servers", brokerList } };

            using (var producer = new Producer<string, string>(config, 
                    new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                for(var i=0; i<1000000; i++){
                    var key = i.ToString();
                    var value = i.ToString();
                    producer.ProduceAsync(topicName, key, value);
                    Console.Write(".");
                    Thread.Sleep(10);
                }
                Console.WriteLine("flushing...");
                producer.Flush(-1); // block until done
            }
            Console.WriteLine("<<< Ending Manual Offset Producer");
        }
    }
}
