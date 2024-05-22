// See https://aka.ms/new-console-template for more information
using Google.Cloud.PubSub.V1;
using Google.Api.Gax.ResourceNames;
using Google.Apis.Auth.OAuth2;
using Grpc.Auth;
using Grpc.Core;
using Google.Cloud.ResourceManager.V3;
using Google.LongRunning;

try
{
    //Manually you hav eto make from google pub sub ashboard
    string projectId = "rpa-pub-sub-services";
    string serviceAccountPath = "C:\\Users\\Mohd Danish\\Downloads\\rpa-pub-sub-services-84875417fab2.json";
    int maximumNoOfMessagesWhilePulling = 10;

    // we can ask from google pub sub
    string topicId = "RPA-Testing-Topic-2";
    string subscriptionId = "RPA-Testing-Subscription_Id_0";
    //string subscriptionId2 = "RPA-Testing-Subscription_Id_2";

    GoogleCredential credential;
    using (var stream = new FileStream(serviceAccountPath, FileMode.Open, FileAccess.Read))
    {
        credential = GoogleCredential.FromStream(stream)
            .CreateScoped("https://www.googleapis.com/auth/cloud-platform");
    }

    ChannelCredentials channelCredentials = credential.ToChannelCredentials();



    //create topic
    await CreateTopicAsync(projectId, topicId, channelCredentials);

    // Create a subscription to the topic
    await CreateSubscriptionAsync(projectId, subscriptionId, topicId,  channelCredentials);

    // Publish a message to the topic
    await PublishMessageAsync(projectId, topicId, "New topic new subscription Id ", channelCredentials);

    //Pull messages from the subscription
    await PullMessagesAsync(projectId, subscriptionId, channelCredentials, maximumNoOfMessagesWhilePulling);
    Console.WriteLine("After first call");
    await PullMessagesAsync(projectId, subscriptionId, channelCredentials, maximumNoOfMessagesWhilePulling);
    Console.WriteLine("After Second call");
    Console.ReadLine();
}


catch (Exception ex)
{
    Console.WriteLine(ex.ToString());
    Console.ReadLine();
}

static async Task CreateProjectAsync(string projectId, string projectName, GoogleCredential credential)
{
    ProjectName parent = new ProjectName("organizations/YOUR_ORGANIZATION_ID"); // Replace with your organization ID
    ProjectsClient projectsClient = await ProjectsClient.CreateAsync();
    Project project = new Project
    {
        ProjectId = projectId,
        Name = projectName,
        Parent = parent.ToString()
    };

    Operation<Project, CreateProjectMetadata> operation = await projectsClient.CreateProjectAsync(project);

    // Wait for the operation to complete
    Operation<Project, CreateProjectMetadata> completedOperation = await operation.PollUntilCompletedAsync();
    Project createdProject = completedOperation.Result;

    Console.WriteLine($"Project {createdProject.Name} created.");
}



static async Task CreateTopicAsync(string projectId, string topicId, ChannelCredentials credentials)
{
    TopicName topicName = TopicName.FromProjectTopic(projectId, topicId);
    PublisherServiceApiClient publisher = new PublisherServiceApiClientBuilder
    {
        ChannelCredentials = credentials
    }.Build();

    try
    {
        Topic topic = await publisher.CreateTopicAsync(topicName);
        Console.WriteLine($"Topic {topic.Name} created.");
    }
    catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
    {
        Console.WriteLine($"Topic {topicName} already exists.");
    }
}

static async Task CreateSubscriptionAsync(string projectId, string subscriptionId, string topicId, ChannelCredentials credentials)
{
    SubscriptionName subscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId);
    TopicName topicName = TopicName.FromProjectTopic(projectId, topicId);
    SubscriberServiceApiClient subscriber = new SubscriberServiceApiClientBuilder
    {
        ChannelCredentials = credentials
    }.Build();

    try
    {
        Subscription subscription = await subscriber.CreateSubscriptionAsync(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);
        Console.WriteLine($"Subscription {subscription.Name} created.");
    }
    catch (RpcException e) when (e.Status.StatusCode == StatusCode.AlreadyExists)
    {
        Console.WriteLine($"Subscription {subscriptionName} already exists.");
    }
}

static async Task PublishMessageAsync(string projectId, string topicId, string message, ChannelCredentials credentials)
{
    TopicName topicName = TopicName.FromProjectTopic(projectId, topicId);
    PublisherClient publisher = await PublisherClient.CreateAsync(topicName, new PublisherClient.ClientCreationSettings(credentials: credentials));

    try
    {
        for (int i = 0;i< 45;i++) {
            string messageId = await publisher.PublishAsync(message + i);
            Console.WriteLine($"Published message ID: {messageId}");
        }   
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
        foreach (ReceivedMessage message in response.ReceivedMessages)
        {
            Console.WriteLine($"Received message: {message.Message.Data.ToStringUtf8()}");
            await subscriber.AcknowledgeAsync(subscriptionName, new List<string> { message.AckId });
        }
    }
    catch (RpcException e)
    {
        Console.WriteLine($"Error pulling messages: {e.Message}");
    }
}




