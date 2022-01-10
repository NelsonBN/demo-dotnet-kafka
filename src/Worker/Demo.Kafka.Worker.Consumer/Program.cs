using Demo.Kafka.Worker.Consumer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using static System.Console;

WriteLine("STARTING CONSUMER WORKER...");


var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) => services.AddHostedService<Worker>())
    .Build();

await host.RunAsync();
