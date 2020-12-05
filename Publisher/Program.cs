using Microsoft.Azure.ServiceBus;
using Models;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading.Tasks;

namespace Publisher
{
    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://sbmsdemo1.servicebus.windows.net/;SharedAccessKeyName=demo1;SharedAccessKey=e8BKqm6hmPIT8fg0mzvKH5uzKGUlO3nqxIys8AlDcfw=;";
        const string TopicName = "msdemotopic";
        private static TopicClient topicClient;

        static void Main(string[] args)
        {
            Console.WriteLine("Publisher!");
            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            topicClient = new TopicClient(ServiceBusConnectionString, TopicName);
            // Send messages to Topic
            //await SendMessagesAsync();
            EmployeeCreateEvent e1 = new EmployeeCreateEvent("E1", "Manager", "Createhandler", Guid.NewGuid(), DateTime.Now);
            EmployeeDeleteEvent e2 = new EmployeeDeleteEvent("E2", "Manager", "Deletehandler", Guid.NewGuid(), DateTime.Now);
            Publish(e1);
            Publish(e2);
            Console.ReadKey();

            await topicClient.CloseAsync();
        }

        static void Publish(IntegrationEvent @event)
        {
            var eventName = @event.GetType().Name.Replace("Event","");
            var jsonMessage = JsonConvert.SerializeObject(@event);
            var body = Encoding.UTF8.GetBytes(jsonMessage);

            var message = new Message
            {
                MessageId = Guid.NewGuid().ToString(),
                Body = body,
                Label = eventName,
            };

            topicClient.SendAsync(message)
                .GetAwaiter()
                .GetResult();
        }

        static async Task SendMessagesAsync()
        {
            Console.WriteLine($"==========================================================================");
            Console.WriteLine("Sending Messages to Topic");
            try
            {
                await Task.WhenAll(
                    SendMessageAsync(label: "Red"),
                    SendMessageAsync(label: "Blue"),
                    SendMessageAsync(label: "Red", correlationId: "important"),
                    SendMessageAsync(label: "Blue", correlationId: "important"),
                    SendMessageAsync(label: "Red", correlationId: "notimportant"),
                    SendMessageAsync(label: "Blue", correlationId: "notimportant"),
                    SendMessageAsync(label: "Green"),
                    SendMessageAsync(label: "Green", correlationId: "important"),
                    SendMessageAsync(label: "Green", correlationId: "notimportant")
                );
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        static async Task SendMessageAsync(string label, string correlationId = null)
        {
            Message message = new Message { Label = label };
            message.UserProperties.Add("Color", label);

            if (correlationId != null)
            {
                message.CorrelationId = correlationId;
            }

            await topicClient.SendAsync(message);
            Console.WriteLine($"Sent Message:: Label: {message.Label}, CorrelationId: {message.CorrelationId ?? message.CorrelationId}");
        }
    }
}
