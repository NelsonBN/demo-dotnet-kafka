using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Kafka.WorkerMultiTopics.Producer;

public class WorkerTopicB : BackgroundService
{
    private readonly string _topicName;
    private readonly ILogger<WorkerTopicB> _logger;
    private readonly IProducer<Null, string> _producer;

    public WorkerTopicB(
        IConfiguration configuration,
        ILogger<WorkerTopicB> logger,
        IProducer<Null, string> producer
    )
    {
        _topicName = configuration.GetValue<string>("TOPIC_NAME_B");
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
                var message = $"Demo message BBBB: {count}";

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

                Thread.Sleep(TimeSpan.FromMilliseconds(200));

                count++;
            }
        }
        catch(ProduceException<Null, string> exception)
        {
            _logger.LogError($"Delivery failed: {exception.Error.Reason}");
        }
    }
}
