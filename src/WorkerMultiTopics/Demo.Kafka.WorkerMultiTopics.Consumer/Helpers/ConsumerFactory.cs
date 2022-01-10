using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Demo.Kafka.WorkerMultiTopics.Consumer.Helpers;

public class ConsumerFactory : IConsumerFactory
{
    private readonly ConsumerConfig _config;
    private readonly ILogger _logger;

    public ConsumerFactory(
        ConsumerConfig config,
        ILoggerFactory loggerFactor
    )
    {
        _config = config;
        _logger = loggerFactor.CreateLogger("ConsumerProxy");
    }

    public IConsumer<Null, TValue> Create<TValue>(string topic)
        => Create<Null, TValue>(topic);

    public IConsumer<TKey, TValue> Create<TKey, TValue>(string topic)
    {
        var consumer = new ConsumerBuilder<TKey, TValue>(_config)
            .SetErrorHandler((_, e) => _logger.LogError($"ERROR: {e.Reason}"))
            .SetStatisticsHandler((_, json) => _logger.LogDebug($"Statistics: {json}"))
            .SetPartitionsAssignedHandler((c, partitions)
                => _logger.LogInformation(
                    "Partitions incrementally assigned: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], all: [" +
                        string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                    "]"))
            .SetPartitionsRevokedHandler((c, partitions) =>
            {
                var remaining = c.Assignment.Where(atp => !partitions.Where(rtp => rtp.TopicPartition == atp).Any());

                _logger.LogInformation(
                    "Partitions incrementally revoked: [" +
                        string.Join(',', partitions.Select(p => p.Partition.Value)) +
                        "], remaining: [" +
                        string.Join(',', remaining.Select(p => p.Partition.Value)) +
                    "]");
            })
            .SetPartitionsLostHandler((c, partitions)
                => _logger.LogInformation($"Partitions were lost: [{string.Join(", ", partitions)}]")
            )
            .Build();

        consumer.Subscribe(topic);

        return consumer;
    }
}
