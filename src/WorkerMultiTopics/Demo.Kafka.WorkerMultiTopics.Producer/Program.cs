using Confluent.Kafka;
using Demo.Kafka.WorkerMultiTopics.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static System.Console;

WriteLine("STARTING PRODUCER MULTI TOPICS...");


var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
        {
            var config = new ProducerConfig
            {
                BootstrapServers = hostContext.Configuration.GetValue<string>("SERVER_ADDRESS")
            };

            services.AddSingleton(
                new ProducerBuilder<Null, string>(config)
                .Build()
            );

            services.AddHostedService<WorkerTopicA>();
            services.AddHostedService<WorkerTopicB>();
        })
    .Build();

await host.RunAsync();
