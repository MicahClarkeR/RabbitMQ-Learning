using Amqp;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using System.Text;

namespace NewTask
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
            RabbitMQ.AMQP.Client.IConnection connection = await environment.CreateConnectionAsync();

            try
            {
                bool cont = true;
                IManagement management = connection.Management();
                IQueueSpecification queue = management.Queue(queueName).Type(QueueType.QUORUM);

                await queue.DeclareAsync();

                IPublisher publisher = await connection.PublisherBuilder().Queue(queueName).BuildAsync();

                try
                {
                    while (cont)
                    {
                        Console.WriteLine("Message:");
                        string messageContent = (args.Length == 0) ? Console.ReadLine() ?? string.Empty : string.Join(" ", args);

                        if (messageContent == string.Empty)
                        {
                            cont = false;
                        }
                        else
                        {
                            var message = new AmqpMessage(Encoding.UTF8.GetBytes(messageContent));

                            PublishResult result = await publisher.PublishAsync(message);

                            Console.WriteLine($"{result.Outcome.State}: {result.Message.BodyAsString()}");
                        }

                        args = [];
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
