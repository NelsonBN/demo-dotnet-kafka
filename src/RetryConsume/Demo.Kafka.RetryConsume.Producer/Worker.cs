using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Kafka.RetryConsume.Producer;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IProducer<Null, string> _producer;
    private readonly string _topicName;

    public Worker(
        IConfiguration configuration,
        ProducerConfig producerConfig,
        ILogger<Worker> logger
    )
    {
        _logger = logger;

        _topicName = configuration.GetValue<string>("TOPIC_NAME");
        _producer = new ProducerBuilder<Null, string>(producerConfig)
            .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            long count = 1;
            while(!stoppingToken.IsCancellationRequested)
            {
                var deliveryResult = await _producer.ProduceAsync(
                    _topicName,
                    new Message<Null, string> { Value = $"[{count}] Demo message..." },
                    stoppingToken
                );

                _logger.LogInformation($"[{count}] Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");

                Thread.Sleep(TimeSpan.FromMilliseconds(500));

                count++;
            }
        }
        catch(ProduceException<Null, string> exception)
        {
            _logger.LogError($"Delivery failed: {exception.Error.Reason}");
        }
    }
}
