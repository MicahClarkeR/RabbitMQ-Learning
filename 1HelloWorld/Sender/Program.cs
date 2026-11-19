using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Text;

namespace Sender
{
    internal class Program
    {
        /// <summary>
        /// https://www.rabbitmq.com/tutorials/tutorial-one-dotnet-amqp10
        /// </summary>
        const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";

        static async Task Main(string[] args)
        {

            ConnectionSettings settings = ConnectionSettingsBuilder.Create()
                .Uri(new Uri(brokerUri))
                .ContainerId("tutorial-send")
                .Build();

            IEnvironment environment = AmqpEnvironment.Create(settings);
            IConnection connection = await environment.CreateConnectionAsync();

            try
            {
                IManagement management = connection.Management();
                IQueueSpecification specification = management.Queue("hello").Type(QueueType.QUORUM);

                await specification.DeclareAsync();

                IPublisher publisher = await connection.PublisherBuilder().Queue("hello").BuildAsync();

                try
                {
                    string? input = null;

                    while(input != string.Empty)
                    {
                        Console.WriteLine("Type message:");

                        input = Console.ReadLine() ?? string.Empty;

                        if (input != string.Empty)
                        {
                            IMessage message = new AmqpMessage(Encoding.UTF8.GetBytes(input));

                            PublishResult result = await publisher.PublishAsync(message);

                            if (result.Outcome.State != OutcomeState.Accepted)
                            {
                                Console.Error.WriteLine($"Unexpectd publsih outcome: {result.Outcome.State}");
                                Environment.Exit(1);
                            }

                            Console.WriteLine($"  Sent: {input}");
                            Console.WriteLine($"Result: {result.Message.BodyAsString()}");
                        }
                    }
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
