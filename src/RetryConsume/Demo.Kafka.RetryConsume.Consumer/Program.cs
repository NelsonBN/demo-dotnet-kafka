using Confluent.Kafka;
using Demo.Kafka.RetryConsume.Consumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static System.Console;

WriteLine("STARTING CONSUMER RETRY CONSUME...");


var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services.AddSingleton(new ConsumerConfig
        {
            GroupId = hostContext.Configuration.GetValue<string>("GROUD_ID"),
            BootstrapServers = hostContext.Configuration.GetValue<string>("SERVER_ADDRESS"),
            AutoOffsetReset = AutoOffsetReset.Earliest
        });


        services.AddHostedService<Worker>();
    })
    .Build();

await host.RunAsync();
