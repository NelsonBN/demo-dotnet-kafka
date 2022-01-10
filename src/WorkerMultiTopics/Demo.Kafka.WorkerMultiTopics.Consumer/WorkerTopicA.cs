using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Demo.Kafka.WorkerMultiTopics.Consumer.Helpers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Kafka.WorkerMultiTopics.Consumer;

public class WorkerTopicA : BackgroundService
{
    private readonly ILogger<WorkerTopicA> _logger;
    private readonly IConsumer<Null, string> _consumer;

    public WorkerTopicA(
        IConfiguration configuration,
        ILogger<WorkerTopicA> logger,
        IConsumerFactory factory
    )
    {
        _logger = logger;

        _consumer = factory.Create<Null, string>(configuration.GetValue<string>("TOPIC_NAME_A"));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while(!stoppingToken.IsCancellationRequested)
        {
            await Task.Run(() =>
            {
                try
                {
                    var consumeResult = _consumer.Consume(stoppingToken);
                    _logger.LogInformation($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");

                    if(consumeResult.IsPartitionEOF)
                    {
                        _logger.LogDebug($"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");
                    }

                }
                catch(ConsumeException exception)
                {
                    _logger.LogError($"Error occured: {exception.Error.Reason}");
                }
            }, stoppingToken);
        }
    }
}
