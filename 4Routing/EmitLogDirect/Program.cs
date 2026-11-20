using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Text;

namespace EmitLogDirect
{
    internal class Program
    {
        const string brokerUri = "amqp://guest:guest@localhost:5672/%2f";
        const string exchangeName = "logs_direct";

        static async Task Main(string[] args)
        {
            //Get 'severity' to log message with by the log receiver, i.e. 'warning', 'error', 'info'
            string severity = GetSeverity(args);
            string message = GetMessage(args);

            ConnectionSettings settings = ConnectionSettingsBuilder.Create()
                .Uri(new Uri(brokerUri))
                .ContainerId("emitlogdirect")
                .Build();

            IEnvironment environment = AmqpEnvironment.Create(settings);
            IConnection connection = await environment.CreateConnectionAsync();

            try
            {
                IManagement management = connection.Management();
                IExchangeSpecification specification = management.Exchange(exchangeName).Type("direct");

                await specification.DeclareAsync();

                IPublisher publisher = await connection.PublisherBuilder().Exchange(exchangeName).Key(severity).BuildAsync();

                try
                {
                    AmqpMessage amqpMessage = new AmqpMessage(Encoding.UTF8.GetBytes(message));
                    PublishResult publishResult = await publisher.PublishAsync(amqpMessage);

                    switch(publishResult.Outcome.State)
                    {
                        case OutcomeState.Accepted:
                            break;
                        case OutcomeState.Released:
                            Console.Error.WriteLine($"Message Released: {publishResult.Message.BodyAsString()}");
                            Console.ReadKey();
                            Environment.Exit(0);
                            break;
                        case OutcomeState.Rejected:
                            Console.Error.WriteLine($"Message Rejected: {publishResult.Message.BodyAsString()}");
                            Console.ReadKey();
                            Environment.Exit(0);
                            break;
                    }

                    Console.WriteLine($"Message ({severity}): {message}");
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

        static string GetSeverity(string[] strings) => strings.Length < 1 ? "info" : strings[0];

        static string GetMessage(string[] strings)
        {
            if (strings.Length < 2)
            {
                return "Hello World!";
            }

            return string.Join(" ", strings.Skip(1));
        }
    }
}
