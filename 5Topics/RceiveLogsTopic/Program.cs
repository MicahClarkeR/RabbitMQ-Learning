using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;

namespace ReceiveLogsTopic
{
    internal class Program
    {
        const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
        const string exchangeName = "logs_topic";

        static async Task Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.Error.WriteLine("Usage: ReceiveLogsTopic [keys]");
                Console.Error.WriteLine("Guide: Type keys seperated by spaces, wildcard utilities below:");
                Console.Error.WriteLine("- * substitutes exactly one word.");
                Console.Error.WriteLine("- # substitutes zero or more words.");
                Console.Error.WriteLine("- Words are separated by . in routing keys.");
                Console.ReadKey();
                Environment.Exit(1);
            }

            ConnectionSettings settings = ConnectionSettingsBuilder.Create()
                .Uri(new Uri(brokerUri))
                .ContainerId("ReceiveLogsTopic")
                .Build();

            IEnvironment environment = AmqpEnvironment.Create(settings);
            IConnection connection = await environment.CreateConnectionAsync();

            try
            {
                IManagement management = connection.Management();
                IExchangeSpecification exchange = management.Exchange(exchangeName).Type(ExchangeType.TOPIC);

                await exchange.DeclareAsync();

                IQueueSpecification queue = management.Queue().Exclusive(true).AutoDelete(true);
                IQueueInfo info = await queue.DeclareAsync();
                string name = info.Name();

                // Get the 'key' from passed `args` to capture.
                // - * substitutes exactly one word.
                // - # substitutes zero or more words.
                // - Words are separated by . in routing keys.
                foreach (string bindingKey in args)
                {
                    IBindingSpecification binding = management.Binding()
                        .SourceExchange(exchange)
                        .DestinationQueue(name)
                        .Key(bindingKey); // Arg key to bind to.

                    Console.WriteLine($"Binding key: {bindingKey}");
                    await binding.BindAsync();
                }

                IConsumer consumer = await connection.ConsumerBuilder()
                    .Queue(name)
                    .MessageHandler((ctx, message) =>
                    {
                        string body = message.BodyAsString();
                        string routingKey = GetRoutingKey(message);

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