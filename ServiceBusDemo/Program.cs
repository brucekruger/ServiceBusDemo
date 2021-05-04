using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceBusDemo
{
    class Program
    {
        const int numberOfMessages = 5;
        private static string queueName = string.Empty;
        private static string serviceBusConnectionString = string.Empty;
        private static IQueueClient queueClient;

        static async Task Main(string[] args)
        {
            await SetupKeyVaultSettings();

            queueClient = new QueueClient(serviceBusConnectionString, queueName);

            Console.WriteLine("Press ENTER to send messages...");

            for(var i = 0; i < numberOfMessages; i++)
            {
                string messageBody = "Message {i}";
                var message = new Message(Encoding.UTF8.GetBytes(messageBody));
                await queueClient.SendAsync(message);
            }
            Console.ReadKey();

            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
            Console.ReadKey();
        }

        private static async Task SetupKeyVaultSettings()
        {
            var keyVaultName = Environment.GetEnvironmentVariable("KEY_VAULT_NAME");
            var kvUri = $"https://{keyVaultName}.vault.azure.net";

            var client = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());

            var queueNameSecret = await client.GetSecretAsync("QueueName");
            queueName = queueNameSecret.Value.Value;

            var serviceBusConnectionStringSecret = await client.GetSecretAsync("ServiceBusConnectionString");
            serviceBusConnectionString = serviceBusConnectionStringSecret.Value.Value;
        }

        private static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {
            Console.WriteLine($"Received message: SequenceNumber: {message.SystemProperties.SequenceNumber}");
            await queueClient.CompleteAsync(message.SystemProperties.LockToken);
        }

        private static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs}");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}
