using System.Text;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");

// number of events to be sent to the event hub
int numOfEvents = 3;

// The Event Hubs client types are safe to cache and use as a singleton for the lifetime
// of the application, which is best practice when events are being published or read regularly
EventHubProducerClient producerClient = new EventHubProducerClient(
    "hvtien3eventhubns.servicebus.windows.net",
    "myeventhubtestcapture",
    new DefaultAzureCredential());

// Create batch of events
using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

for (int i = 1; i <= numOfEvents; i++){
    if(!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes($"Event {i}")))){
        throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
    }
}

try{
    // Use the producer client to send the batch of events to the event hub
    await producerClient.SendAsync(eventBatch);
     Console.WriteLine($"A batch of {numOfEvents} events has been published.");
}
finally
{
    await producerClient.DisposeAsync();
}