
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Collections;
using System.Text;

namespace Worker
{
    internal class Program
    {
        const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
        const string containerName = "newTaskContainer";
        const string queueName = "taskQueue";


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
                IQueueSpecification queue = management.Queue(queueName).Type(QueueType.QUORUM);

                await queue.DeclareAsync();
                await queue.PurgeAsync();

                IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync();

                IConsumer consumer = await connection.ConsumerBuilder()
                    .Queue(queueName)
                    .InitialCredits(1)
                    .MessageHandler((ctx, message) =>
                    {
                        string body = Encoding.UTF8.GetString(message.Body()!);
                        Console.WriteLine($" [x] Received '{body}'");

                        for (int i = 0; i < body.Length; i++)
                        {
                            Console.WriteLine($"Waiting {body.Length - i}...");
                            Thread.Sleep(1000);
                        }

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
