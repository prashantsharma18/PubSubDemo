using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SubscriberA
{
    class Program
    {
        const string ServiceBusConnectionString = "Endpoint=sb://sbmsdemo1.servicebus.windows.net/;SharedAccessKeyName=demo1;SharedAccessKey=e8BKqm6hmPIT8fg0mzvKH5uzKGUlO3nqxIys8AlDcfw=;";
        const string TopicName = "msdemotopic";
        static ITopicClient topicClient;
        static ISubscriptionClient empCreateFilterSubscriptionClient, empDeleteFilterSubscriptionClient;
        private SubscriptionClient _subscriptionClient;
        private static string INTEGRATION_EVENT_SUFFIX = "Event";

        static void Main(string[] args)
        {
            Console.WriteLine("Subscribers");
            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

            empCreateFilterSubscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, "EmployeeCreate");
            empDeleteFilterSubscriptionClient = new SubscriptionClient(ServiceBusConnectionString, TopicName, "EmployeeDelete");


            // Receive messages from 'allMessagesSubscriptionName'. Should receive all 9 messages 
            await ReceiveMsg(empCreateFilterSubscriptionClient);

            // Receive messages from 'sqlFilterOnlySubscriptionName'. Should receive all messages with Color = 'Red' i.e 3 messages
            await ReceiveMsg(empDeleteFilterSubscriptionClient);

            Console.WriteLine("=========================================================");
            Console.WriteLine("Completed Receiving all messages... Press any key to exit");
            Console.WriteLine("=========================================================");

            Console.ReadKey();

            await empCreateFilterSubscriptionClient.CloseAsync();
            await empDeleteFilterSubscriptionClient.CloseAsync();
            await topicClient.CloseAsync();
        }

        static async Task ReceiveMsg(ISubscriptionClient _subscriptionClient)
        {
            _subscriptionClient.RegisterMessageHandler(
                async (message, token) =>
                {
                    var eventName = $"{message.Label}{INTEGRATION_EVENT_SUFFIX}";
                    var messageData = Encoding.UTF8.GetString(message.Body);
                    var integrationEvent = JsonConvert.DeserializeObject(messageData, Type.GetType(eventName));

                    await _subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
                    
                },
                new MessageHandlerOptions(ExceptionReceivedHandler) { MaxConcurrentCalls = 10, AutoComplete = false });

        }

        private static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            var ex = exceptionReceivedEventArgs.Exception;
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;

            return Task.CompletedTask;
        }
        static async Task ReceiveMessagesAsync(string subscriptionName)
        {
            string subscriptionPath = EntityNameHelper.FormatSubscriptionPath(TopicName, subscriptionName);
            IMessageReceiver subscriptionReceiver = new MessageReceiver(ServiceBusConnectionString, subscriptionPath, ReceiveMode.ReceiveAndDelete);

            Console.WriteLine($"==========================================================================");
            Console.WriteLine($"{DateTime.Now} :: Receiving Messages From Subscription: {subscriptionName}");
            int receivedMessageCount = 0;
            while (true)
            {
                var receivedMessage = await subscriptionReceiver.ReceiveAsync();// TimeSpan.Zero);
                if (receivedMessage != null)
                {
                    object colorProperty;
                    receivedMessage.UserProperties.TryGetValue("Color", out colorProperty);
                    Console.WriteLine($"Color Property = {colorProperty}, CorrelationId = {receivedMessage.CorrelationId ?? receivedMessage.CorrelationId}");
                    receivedMessageCount++;
                }
                else
                {
                    break;
                }
            }

            Console.WriteLine($"{DateTime.Now} :: Received '{receivedMessageCount}' Messages From Subscription: {subscriptionName}");
            Console.WriteLine($"==========================================================================");
        }

       
    }
}
