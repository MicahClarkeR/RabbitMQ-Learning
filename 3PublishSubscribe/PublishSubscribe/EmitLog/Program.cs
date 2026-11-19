using Amqp;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Text;

namespace EmitLog
{
    internal class Program
    {
        const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
        const string containerName = "tutorialEmitLog";
        const string exchangeName = "logging";

        static async Task Main(string[] args)
        {
            ConnectionSettings settings = ConnectionSettingsBuilder.Create()
                                            .Uri(new Uri(brokerUri))
                                            .ContainerId(containerName)
                                            .Build();

            IEnvironment environment = AmqpEnvironment.Create(settings);
            RabbitMQ.AMQP.Client.IConnection connection = await environment.CreateConnectionAsync();

            try
            {
                IManagement management = connection.Management();
                IExchangeSpecification exchangeSpec = management.Exchange(exchangeName).Type("fanout");
                
                
                await exchangeSpec.DeclareAsync();

                IPublisher publisher = await connection.PublisherBuilder().Exchange(exchangeName).BuildAsync();

                try
                {
                    string message = args.Length < 1 ? "Hello World!" : string.Join(" ", args);
                    var amqpMessage = new AmqpMessage(Encoding.UTF8.GetBytes(message));

                    PublishResult result = await publisher.PublishAsync(amqpMessage);

                    if (result.Outcome.State == OutcomeState.Accepted)
                    {
                        Console.WriteLine($"Accepted: {result.Message.BodyAsString()}");
                    }
                    else if (result.Outcome.State == OutcomeState.Released)
                    {
                        Console.WriteLine($"Released: {result.Message.BodyAsString()}");
                    }
                    else if (result.Outcome.State == OutcomeState.Rejected)
                    {
                        Console.Error.WriteLine($"Rejected: {result.Message.BodyAsString()}");
                    }
                    else
                    {
                        Console.WriteLine($"Unexpected: {result.Message.BodyAsString()}");
                    }

                    Console.Read();
                    Environment.Exit(0);
                }
                finally
                {
                    await publisher.CloseAsync();
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
