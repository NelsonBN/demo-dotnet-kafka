using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Kafka.Worker.Consumer;

public class Worker : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<Worker> _logger;
    private readonly IConsumer<Null, string> _consumer;

    public Worker(
        IConfiguration configuration,
        ILogger<Worker> logger
    )
    {
        _configuration = configuration;
        _logger = logger;

        var config = new ConsumerConfig
        {
            GroupId = _configuration.GetValue<string>("GROUD_ID"),
            BootstrapServers = _configuration.GetValue<string>("SERVER_ADDRESS"),
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        _consumer = new ConsumerBuilder<Null, string>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(_configuration.GetValue<string>("TOPIC_NAME"));

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
