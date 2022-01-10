using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Kafka.WorkerMultiTopics.Producer;

public class WorkerTopicA : BackgroundService
{
    private readonly string _topicName;
    private readonly ILogger<WorkerTopicA> _logger;
    private readonly IProducer<Null, string> _producer;

    public WorkerTopicA(
        IConfiguration configuration,
        ILogger<WorkerTopicA> logger,
        IProducer<Null, string> producer
    )
    {
        _topicName = configuration.GetValue<string>("TOPIC_NAME_A");
        _logger = logger;

        _producer = producer;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var count = 1;
            while(!stoppingToken.IsCancellationRequested)
            {
                var message = $"Demo message AAAA: {count}";

                await _producer.ProduceAsync(
                    _topicName,
                    new Message<Null, string> { Value = message },
                    stoppingToken
                )
                .ContinueWith(task =>
                {
                    if(task.IsFaulted)
                    {
                        _logger.LogError(task.Exception.InnerException, $"[{count}] Error producing message");
                    }
                    else
                    {
                        _logger.LogInformation($"[{count}] Delivered '{task.Result.Value}' to '{task.Result.TopicPartitionOffset}'");
                    }
                }, stoppingToken);

                Thread.Sleep(TimeSpan.FromSeconds(1));

                count++;
            }
        }
        catch(ProduceException<Null, string> exception)
        {
            _logger.LogError($"Delivery failed: {exception.Error.Reason}");
        }
    }
}
