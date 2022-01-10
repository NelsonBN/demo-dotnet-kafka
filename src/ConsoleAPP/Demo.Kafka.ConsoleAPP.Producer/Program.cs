using System.Threading;
using Confluent.Kafka;
using static System.Console;

WriteLine("STARTING PRODUCER CONSOLE...");

const string TOPIC_NAME = "demo.consoleapp.topic";
const string SERVER_ADDRESS = "localhost:9092";

var config = new ProducerConfig
{
    BootstrapServers = SERVER_ADDRESS
};


using var producer = new ProducerBuilder<Null, string>(config).Build();


try
{
    var count = 1;
    while(true)
    {
        var deliveryResult = await producer.ProduceAsync(
            TOPIC_NAME,
            new Message<Null, string> { Value = $"Demo message: {count}" }
        );

        WriteLine($"[{count}] Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");

        Thread.Sleep(500);

        count++;
    }
}
catch(ProduceException<Null, string> exception)
{
    WriteLine($"Delivery failed: {exception.Error.Reason}");
}

WriteLine("ENDED PRODUCE!");
