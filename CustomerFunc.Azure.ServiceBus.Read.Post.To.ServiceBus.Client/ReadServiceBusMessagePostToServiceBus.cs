using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace CustomerFunc.Azure.ServiceBus.Read.Post.To.ServiceBus.Client
{
    public static class ReadServiceBusMessagePostToServiceBus
    {
        private static IConfigurationRoot config;

        [FunctionName("read-servicebus-message-post-to-servicebus")]
        public async static Task Run([ServiceBusTrigger("text-file-topic", "%SubscriptionName%", Connection = "ServiceBusConnectionString")]string mySbMsg, ExecutionContext context, string CorrelationId, string MessageId, ILogger log)
        {
            config = new ConfigurationBuilder().SetBasePath(context.FunctionAppDirectory).AddJsonFile("local.settings.json", optional: true, reloadOnChange: false).AddEnvironmentVariables().Build();
            if(string.IsNullOrWhiteSpace(mySbMsg))
            {
                log.LogInformation("No message content to process...");
                //await MessageReceiver.DeadLetterAsync(mySbMsg, "Empty Message Content...");
            }

            string TargetTopicName = "text-file-topic";
            await SendMessageToBus(mySbMsg, TargetTopicName, "Client1-Processed",CorrelationId, MessageId);
        }

        private static async Task SendMessageToBus(string messageToSend, string topic, string customerType, string CorrelationId,string MessageId)
        {
            try
            {
                Message message = new Message(Encoding.UTF8.GetBytes(messageToSend));
                message.UserProperties.Add("MessageFrom", customerType);
                message.CorrelationId = CorrelationId;
                message.MessageId = MessageId;

                TopicClient topicClient = new TopicClient(config["ServiceBusConnectionStringTarget"], topic);
                await topicClient.SendAsync(message);
                Console.WriteLine($"Message dropped to Service bus Topic {topic}");
                //await MessageReceiver.CompleteAsync(messageToSend);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
