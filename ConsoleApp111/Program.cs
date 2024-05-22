// See https://aka.ms/new-console-template for more information
using Google.Cloud.PubSub.V1;
using Google.Api.Gax.ResourceNames;
using Google.Apis.Auth.OAuth2;
using Grpc.Auth;
using Grpc.Core;
using Google.Cloud.ResourceManager.V3;
using Google.LongRunning;
using Google.Protobuf.WellKnownTypes;

try
{
    //Manually you hav eto make from google pub sub ashboard
    string projectId = "rpa-pub-sub-services";
    string serviceAccountPath = "C:\\Users\\Mohd Danish\\Downloads\\rpa-pub-sub-services-84875417fab2.json";
    int maximumNoOfMessagesWhilePulling = 10;
    int retentionDaysForTopic = 30;

    // we can ask from google pub sub
    string topicId = "RPA-Testing-Attributes-Topic-with-retention-in-days-2";
    string newSubscriptionId = "RPA-Testing-Attributes-subs-new-with-retention-in-days-2";
    string oldSubscriptionId = "RPA-Testing-Attributes-subs-old-with-retention-in-days-2";

    GoogleCredential credential;
    using (var stream = new FileStream(serviceAccountPath, FileMode.Open, FileAccess.Read))
    {
        credential = GoogleCredential.FromStream(stream)
            .CreateScoped("https://www.googleapis.com/auth/cloud-platform");
    }
    ChannelCredentials channelCredentials = credential.ToChannelCredentials();

    //create topic
    await CreateTopicAsync(projectId, topicId, channelCredentials, retentionDaysForTopic);

    // Create Subscription
    await CreateSubscriptionWithFilterAsync(projectId, topicId, newSubscriptionId, "attributes.status=\"new\"", channelCredentials);
    await CreateSubscriptionWithFilterAsync(projectId, topicId, oldSubscriptionId, "attributes.status=\"old\"", channelCredentials);

    // Publish messages with different attributes
    await PublishMessageWithAttributesAsync(projectId, topicId, "New Message 1", new Dictionary<string, string> { { "status", "new" } }, channelCredentials);
    await PublishMessageWithAttributesAsync(projectId, topicId, "Old Message 1", new Dictionary<string, string> { { "status", "old" } }, channelCredentials);

    // Pull messages from each subscription
    Console.WriteLine("Pulling messages from new subscription:");
    await PullMessagesAsync(projectId, newSubscriptionId, channelCredentials, maximumNoOfMessagesWhilePulling);

    Console.WriteLine("Pulling messages from old subscription:");
    await PullMessagesAsync(projectId, oldSubscriptionId, channelCredentials, maximumNoOfMessagesWhilePulling);

    Console.ReadLine();
}


catch (Exception ex)
{
    Console.WriteLine(ex.ToString());
    Console.ReadLine();
}


static async Task CreateTopicAsync(string projectId, string topicId, ChannelCredentials credentials, int retentionDays)
{
    TopicName topicName = TopicName.FromProjectTopic(projectId, topicId);
    PublisherServiceApiClient publisher = new PublisherServiceApiClientBuilder
    {
        ChannelCredentials = credentials
    }.Build();

    TimeSpan messageRetentionDuration = TimeSpan.FromDays(retentionDays);

    try
    {
        //Topic topic = await publisher.CreateTopicAsync(topicName);
        //Console.WriteLine($"Topic {topic.Name} created.");
        
        Topic topic = new Topic
        {
            TopicName = topicName,
            MessageRetentionDuration = Google.Protobuf.WellKnownTypes.Duration.FromTimeSpan(messageRetentionDuration)
        };

        Topic createdTopic = await publisher.CreateTopicAsync(topic);
        Console.WriteLine($"Topic {createdTopic.Name} created with a retention period of {retentionDays} days.");
        
    }
    catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
    {
        Console.WriteLine($"Topic {topicName} already exists.");
    }
}
    

static async Task CreateSubscriptionWithFilterAsync(string projectId, string topicId, string subscriptionId, string filter, ChannelCredentials credentials)
{
    SubscriptionName subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);
    TopicName topicName = TopicName.FromProjectTopic(projectId, topicId);
    SubscriberServiceApiClient subscriber = new SubscriberServiceApiClientBuilder
    {
        ChannelCredentials = credentials
    }.Build();
    try
    {
        Subscription subscription = new Subscription
        {
            SubscriptionName = subscriptionName,
            TopicAsTopicName = topicName,
            Filter = filter,
            AckDeadlineSeconds = 60

        };
        subscription = await subscriber.CreateSubscriptionAsync(subscription);
        Console.WriteLine($"Subscription {subscription.Name} created with filter: {filter}");
    }
    catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
    {
        Console.WriteLine($"Subscription {subscriptionName} already exists.");
    }
}

static async Task PublishMessageWithAttributesAsync(string projectId, string topicId, string message, IDictionary<string, string> attributes, ChannelCredentials credentials)
{
    TopicName topicName = TopicName.FromProjectTopic(projectId, topicId);
    PublisherClient publisher = await PublisherClient.CreateAsync(topicName, new PublisherClient.ClientCreationSettings(credentials: credentials));

    try
    {
        PubsubMessage pubsubMessage = new PubsubMessage
        {
            Data = Google.Protobuf.ByteString.CopyFromUtf8(message),
            Attributes = { attributes }
        };

        string messageId = await publisher.PublishAsync(pubsubMessage);
        Console.WriteLine($"Published message ID: {messageId} with attributes: {string.Join(", ", attributes)}");
    }
    catch (Exception e)
    {
        Console.WriteLine($"Error publishing message: {e.Message}");
    }
}

static async Task PullMessagesAsync(string projectId, string subscriptionId, ChannelCredentials credentials, int maximumNoOfMessagesWhilePulling)
{
    SubscriptionName subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);
    SubscriberServiceApiClient subscriber = new SubscriberServiceApiClientBuilder
    {
        ChannelCredentials = credentials
    }.Build();

    try
    {
        PullResponse response = await subscriber.PullAsync(subscriptionName, returnImmediately: false, maxMessages: maximumNoOfMessagesWhilePulling);
        foreach (ReceivedMessage receivedMessage in response.ReceivedMessages)
        {
            PubsubMessage message = receivedMessage.Message;
            string messageData = message.Data.ToStringUtf8();
            var attributes = message.Attributes;

            Console.WriteLine($"Received message: {messageData}");
            foreach (var attribute in attributes)
            {
                Console.WriteLine($"Attribute - {attribute.Key}: {attribute.Value}");
            }

            // Acknowledge the message
            await subscriber.AcknowledgeAsync(subscriptionName, new List<string> { receivedMessage.AckId });
        }
    }
    catch (RpcException e)
    {
        Console.WriteLine($"Error pulling messages: {e.Message}");
    }
}



