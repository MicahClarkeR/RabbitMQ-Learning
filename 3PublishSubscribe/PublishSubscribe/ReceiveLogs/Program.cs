using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace ReceiveLogs
{
    internal class Program
    {
        const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
        const string containerName = "tutorialReceiveLog";
        const string exchangeName = "logging";

        static async Task Main(string[] args)
        {
            ConnectionSettings settings = ConnectionSettingsBuilder.Create()
                                            .Uri(new Uri(brokerUri))
                                            .ContainerId(containerName)
                                            .Build();

            IEnvironment environment = AmqpEnvironment.Create(settings);
            IConnection connection = await environment.CreateConnectionAsync();

            try
            {
                IManagement management = connection.Management();
                IExchangeSpecification exchange = management.Exchange(exchangeName).Type("fanout");

                IQueueSpecification tempQueue = management.Queue().Exclusive(true).AutoDelete(true);
                IQueueInfo queueInfo = await tempQueue.DeclareAsync();
                string queueName = queueInfo.Name();

                IBindingSpecification binding = management.Binding()
                    .SourceExchange(exchange)
                    .DestinationQueue(queueName)
                    .Key(string.Empty);

                await binding.BindAsync();

                IConsumer consumer = await connection.ConsumerBuilder()
                    .Queue(queueName)
                    .MessageHandler((ctx, message) =>
                    {
                        string body = message.BodyAsString();
                        Console.WriteLine($" [x] Received '{body}'");
                        ctx.Accept();
                        return Task.CompletedTask;
                    })
                    .BuildAndStartAsync();

                try
                {
                    Console.WriteLine(" [*] Waiting for messages. To exit press CTRL+C");
                    using var cts = new CancellationTokenSource();
                    Console.CancelKeyPress += (_, e) =>
                    {
                        e.Cancel = true;
                        cts.Cancel();
                    };
                    await Task.Delay(Timeout.Infinite, cts.Token);
                }
                catch (OperationCanceledException)
                {
                }
                finally
                {
                    await consumer.CloseAsync();
                }
            }
            finally
            {
                await connection.CloseAsync();
                await environment.CloseAsync();
            }
        }
    }
}
