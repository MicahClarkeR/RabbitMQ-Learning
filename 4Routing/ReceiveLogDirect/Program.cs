using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace ReceiveLogDirect
{
    internal class Program
    {
        const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
        const string exchangeName = "logs_direct";

        static async Task Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.Error.WriteLine("Usage: ReceiveLogsDirect [info] [warning] [error]");
                Console.ReadKey();
                Environment.Exit(1);
            }

            ConnectionSettings settings = ConnectionSettingsBuilder.Create()
                .Uri(new Uri(brokerUri))
                .ContainerId("receivelogdirect")
                .Build();

            IEnvironment environment = AmqpEnvironment.Create(settings);
            IConnection connection = await environment.CreateConnectionAsync();

            try
            {
                IManagement management = connection.Management();
                IExchangeSpecification specification = management.Exchange(exchangeName).Type("direct");

                await specification.DeclareAsync();

                IQueueSpecification queue = management.Queue().Exclusive(true).AutoDelete(true);
                IQueueInfo info = await queue.DeclareAsync();
                string name = info.Name();

                // Grab 'severity' from start-up args, i.e. 'warning', 'error', 'info'
                foreach (string severity in args)
                {
                    IBindingSpecification binding = management.Binding()
                        .SourceExchange(specification)
                        .DestinationQueue(name)
                        .Key(severity); // Create queue binding for passsed severity.

                    await binding.BindAsync();
                }

                IConsumer consumer = await connection.ConsumerBuilder()
                    .Queue(name)
                    .MessageHandler((ctx, message) =>
                    {
                        string body = message.BodyAsString();
                        string routingKey = GetRoutingKey(message); // Result will be 'severity', i.e. 'warning', 'error', 'info'

                        Console.WriteLine($" Received (Key: '{routingKey}'): {body}");
                        ctx.Accept();

                        return Task.CompletedTask;
                    })
                    .BuildAndStartAsync();

                try
                {
                    Console.WriteLine(" Waiting for messages. To exit press CTRL+C");

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

        static string GetRoutingKey(IMessage message)
        {
            object? routingKey = message.Annotation("x-routing-key");
            
            if (routingKey != null)
                return routingKey.ToString() ?? "";

            return message.Subject() ?? "";
        }
    }
}
