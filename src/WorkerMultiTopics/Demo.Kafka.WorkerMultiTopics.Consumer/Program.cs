using Confluent.Kafka;
using Demo.Kafka.WorkerMultiTopics.Consumer;
using Demo.Kafka.WorkerMultiTopics.Consumer.Helpers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static System.Console;

WriteLine("STARTING CONSUMER MULTI TOPICS...");


var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services.AddSingleton(new ConsumerConfig
        {
            GroupId = hostContext.Configuration.GetValue<string>("GROUD_ID"),
            BootstrapServers = hostContext.Configuration.GetValue<string>("SERVER_ADDRESS"),
            AutoOffsetReset = AutoOffsetReset.Earliest
        });

        services.AddSingleton<IConsumerFactory, ConsumerFactory>();


        services.AddHostedService<WorkerTopicA>();
        services.AddHostedService<WorkerTopicB>();
    })
    .Build();

await host.RunAsync();
