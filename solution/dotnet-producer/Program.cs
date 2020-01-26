namespace DotnetProducer
{
    using System;
    using System.IO;
    using System.Threading;
    using Confluent.Kafka;

    /// <summary>
    /// Dotnet producer.
    /// </summary>
    public class Program
    {
        private const string DriverFilePrefix = "./drivers/";
        private const string KafkaTopic = "driver-positions";

        /// <summary>
        /// Main method for console app.
        /// </summary>
        /// <param name="args">No arguments used.</param>
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting .net producer.");

            // Configure the location of the bootstrap server, and Confluent interceptors
            // and a partitioner compatible with Java - see https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
            var producerConfig = new ProducerConfig
            {
              // TODO: configure the location of the bootstrap server
              BootstrapServers = "kafka:9092",
              PluginLibraryPaths = "monitoring-interceptor",
              Partitioner = Partitioner.Murmur2Random,
            };

            // Load a driver id from an environment variable
            // if it isn't present use "driver-2"
            string driverId = System.Environment.GetEnvironmentVariable("DRIVER_ID");
            driverId = (!string.IsNullOrEmpty(driverId)) ? driverId : "driver-2";

            Action<DeliveryReport<string, string>> handler = r =>
                Console.WriteLine(!r.Error.IsError
                    ? $"Sent Key:{r.Message.Key} Value:{r.Message.Value}"
                    : $"Delivery Error: {r.Error.Reason}");

            using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
            {
                Console.CancelKeyPress += (sender, e) =>
                {
                    // wait for up to 10 seconds for any inflight messages to be delivered.
                    Console.WriteLine("Flushing producer and exiting.");
                    producer.Flush(TimeSpan.FromSeconds(10));
                };

                var lines = File.ReadAllLines(Path.Combine(DriverFilePrefix, driverId + ".csv"));
                int i = 0;
                // Loop forever over the driver CSV file..
                while (true)
                {
                    string line = lines[i];
                    try
                    {
                        // TODO: populate the message object
                        var message = new Message<string, string> { Key = driverId, Value = line };
                        // TODO: write the lat/long position to a Kafka topic
                        // TODO: configure handler as a callback to print the key and value
                        producer.Produce(KafkaTopic, message, handler);
                    }
                    catch (ProduceException<string, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                        break;
                    }

                    Thread.Sleep(1000);
                    i = (i + 1) % lines.Length;
                }
            }
        }
    }
}
