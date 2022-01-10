using Confluent.Kafka;
using Demo.Kafka.RetryConsume.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static System.Console;

WriteLine("STARTING PRODUCER RETRY CONSUME...");


var host = Host.CreateDefaultBuilder(args)
     .ConfigureServices((hostContext, services) =>
     {
         services.AddSingleton(new ProducerConfig
         {
             BootstrapServers = hostContext.Configuration.GetValue<string>("SERVER_ADDRESS")
         });


         services.AddHostedService<Worker>();
     })
    .Build();

await host.RunAsync();
