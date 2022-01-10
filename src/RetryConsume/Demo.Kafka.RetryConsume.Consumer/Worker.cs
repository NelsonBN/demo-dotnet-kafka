using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Kafka.RetryConsume.Consumer;

public class Worker : BackgroundService
{
    private const int MAX_RETRIES = 3;

    private readonly ILogger<Worker> _logger;
    private readonly IConsumer<Null, string> _consumer;

    public Worker(
        IConfiguration configuration,
        ILogger<Worker> logger,
        ConsumerConfig config
    )
    {
        _logger = logger;

        _consumer = new ConsumerBuilder<Null, string>(config).Build();
        _consumer.Subscribe(configuration.GetValue<string>("TOPIC_NAME"));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var count = 1;
        var countRetries = 0;

        while(!stoppingToken.IsCancellationRequested)
        {
            await Task.Run(() =>
            {
                ConsumeResult<Null, string> consumeResult = null;

                try
                {
                    consumeResult = _consumer.Consume(stoppingToken);

                    if(count % 2 == 0)
                    {
                        throw new Exception("Test error");
                    }

                    _logger.LogInformation($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'.");

                    countRetries = 0;
                }
                catch(Exception exception)
                {
                    _logger.LogError($"Test error: {exception.Message}");

                    countRetries++;
                    if(countRetries < MAX_RETRIES)
                    {
                        _consumer.Assign(consumeResult.TopicPartitionOffset);
                    }
                    else
                    {
                        throw new OperationCanceledException($"Too many consume retries {MAX_RETRIES}");
                    }
                }

            }, stoppingToken);

            count++;
        }
    }
}
