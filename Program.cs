using System;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.SimpleNotificationService;
using System.Collections.Generic;
using Amazon.Auth.AccessControlPolicy;
using System.Threading;

namespace SqsPolicyRepro
{
    class Program
    {
        static long fileTime = DateTime.UtcNow.ToFileTime();
        static string EndpointName = $"{fileTime}Sqs.Policy.Tests";
        static string QueueName = EndpointName.Replace(".", "-");

        static string Topic1 = $"{fileTime}topic1";
        static string Topic2 = $"{fileTime}topic2";

        // change to 1 to make problem disappear
        static SemaphoreSlim limitter = new SemaphoreSlim(2);

        static async Task Main(string[] args)
        {
            using var sqsClient = new AmazonSQSClient();
            using var sqsClient2 = new AmazonSQSClient();
            using var snsClient = new AmazonSimpleNotificationServiceClient();
            using var snsClient2 = new AmazonSimpleNotificationServiceClient();

            var queueUrl = await CreateQueue(sqsClient, QueueName);
            var topic1Arn = await CreateTopic(snsClient, Topic1);
            var topic2Arn = await CreateTopic(snsClient, Topic2);

            async Task<string> Subscribe(AmazonSQSClient queueClient, AmazonSimpleNotificationServiceClient notificationClient, string topicArn)
            {
                try
                {
                    await limitter.WaitAsync();
                    return await notificationClient.SubscribeQueueAsync(topicArn, queueClient, queueUrl);
                }
                finally
                {
                    limitter.Release();
                }
            }

            var subscribeTask = new List<Task<string>> {
                Subscribe(sqsClient, snsClient, topic1Arn), // feel free to experiment here with multiple clients, doesn't make the bug go away
                Subscribe(sqsClient, snsClient, topic2Arn) // feel free to experiment here with multiple clients, doesn't make the bug go away
            };
            await Task.WhenAll(subscribeTask);

            // give some time to settle although this is not necessary
            await Task.Delay(5000);

            var queueAttributes = await sqsClient.GetQueueAttributesAsync(queueUrl, new List<string> { "All" });

            var policyAsPrettyJson = Policy.FromJson(queueAttributes.Attributes["Policy"]).ToJson(prettyPrint: true);

            Console.WriteLine(policyAsPrettyJson);
            var wasTopicOneContained = policyAsPrettyJson.Contains(topic1Arn);
            var wasTopicTwoContained = policyAsPrettyJson.Contains(topic2Arn);
            Console.WriteLine();
            Console.WriteLine(wasTopicOneContained ? "Topic1 found in policy" : "Topic1 NOT found in policy!");
            Console.WriteLine(wasTopicTwoContained ? "Topic2 found in policy" : "Topic2 NOT found in policy!");
            Console.WriteLine("Hit enter to delete all created ressources");
            Console.ReadLine();

            Console.WriteLine("Deleting all ressources");
            await Cleanup(sqsClient, snsClient, subscribeTask, topic1Arn, topic2Arn, queueUrl);
            Console.WriteLine("Deleted all ressources");
            Console.WriteLine("Hit enter to exit");
            Console.ReadLine();
        }

        private static async Task Cleanup(AmazonSQSClient sqsClient, AmazonSimpleNotificationServiceClient snsClient, List<Task<string>> subscribeTask, string topic1Arn, string topic2Arn, string queueUrl)
        {
            foreach (var task in subscribeTask)
            {
                await snsClient.UnsubscribeAsync(task.Result);
            }

            await snsClient.DeleteTopicAsync(topic1Arn);
            await snsClient.DeleteTopicAsync(topic2Arn);
            await sqsClient.DeleteQueueAsync(queueUrl);
        }

        static async Task<string> CreateQueue(AmazonSQSClient client, string queueName)
        {
            var sqsRequest = new CreateQueueRequest
            {
                QueueName = queueName
            };
            var createQueueResponse = await client.CreateQueueAsync(sqsRequest).ConfigureAwait(false);
            var queueUrl = createQueueResponse.QueueUrl;
            return queueUrl;
        }

        static async Task<string> CreateTopic(AmazonSimpleNotificationServiceClient client, string topicName)
        {
            var response = await client.CreateTopicAsync(topicName);
            return response.TopicArn;
        }
    }
}
