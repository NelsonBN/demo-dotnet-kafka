using System;
using System.Threading;
using Confluent.Kafka;
using static System.Console;

WriteLine("STARTING CONSUMER CONSOLE APP...");

const string TOPIC_NAME = "demo.consoleapp.topic";
const string SERVER_ADDRESS = "localhost:9092";
const string GROUD_ID = "demo.consoleapp.group";

var config = new ConsumerConfig
{
    GroupId = GROUD_ID,
    BootstrapServers = SERVER_ADDRESS,
    AutoOffsetReset = AutoOffsetReset.Earliest
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe(TOPIC_NAME);


var cancellationToken = new CancellationTokenSource();
CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cancellationToken.Cancel();
};


try
{
    while(true)
    {
        try
        {
            var consumeResult = consumer.Consume(cancellationToken.Token);
            WriteLine($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");
        }
        catch(ConsumeException exception)
        {
            WriteLine($"Error occured: {exception.Error.Reason}");
        }
    }
}
catch(OperationCanceledException)
{
    consumer.Close();
}

WriteLine("ENDED CONSUMER!");
