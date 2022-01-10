using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Demo.Kafka.WorkerMultiTopics.Consumer.Helpers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Kafka.WorkerMultiTopics.Consumer;

public class WorkerTopicB : BackgroundService
{
    private readonly ILogger<WorkerTopicB> _logger;
    private readonly IConsumer<Null, string> _consumer;

    public WorkerTopicB(
        IConfiguration configuration,
        ILogger<WorkerTopicB> logger,
        IConsumerFactory factory
    )
    {
        _logger = logger;

        _consumer = factory.Create<string>(configuration.GetValue<string>("TOPIC_NAME_B"));
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

                }
                catch(ConsumeException exception)
                {
                    _logger.LogError($"Error occured: {exception.Error.Reason}");
                }
            }, stoppingToken);
        }
    }
}
