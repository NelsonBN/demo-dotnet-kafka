using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Demo.Kafka.Worker.Producer;

public class Worker : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<Worker> _logger;
    private readonly IProducer<Null, string> _producer;

    public Worker(
        IConfiguration configuration,
        ILogger<Worker> logger
    )
    {
        _configuration = configuration;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _configuration.GetValue<string>("SERVER_ADDRESS")
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            var count = 1;
            while(!stoppingToken.IsCancellationRequested)
            {
                var deliveryResult = await _producer.ProduceAsync(
                    _configuration.GetValue<string>("TOPIC_NAME"),
                    new Message<Null, string> { Value = $"Demo message: {count}" },
                    stoppingToken
                );

                _logger.LogInformation($"[{count}] Delivered '{deliveryResult.Value}' to '{deliveryResult.TopicPartitionOffset}'");

                Thread.Sleep(10);

                count++;
            }
        }
        catch(ProduceException<Null, string> exception)
        {
            _logger.LogError($"Delivery failed: {exception.Error.Reason}");
        }
    }
}
