// See https://aka.ms/new-console-template for more information
using Azure.Data.SchemaRegistry;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using com.azure.schemaregistry.samples;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;

Console.WriteLine("Hello, World!");

// connection string to the Event Hubs namespace
const string connectionString = "Endpoint=sb://hvtien3eventhubns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DB2bfCPNJHHzJJIyjAjpHntmo6Oiu+zsh+AEhAlUAFs=";

// name of the event hub
const string eventHubName = "hvtien3eventhub";

// Schema Registry endpoint 
const string schemaRegistryEndpoint = "hvtien3eventhubns.servicebus.windows.net";

// name of the consumer group   
const string schemaGroup = "hvtien3schemagroup";


// The Event Hubs client types are safe to cache and use as a singleton for the lifetime
// of the application, which is best practice when events are being published or read regularly.
EventHubProducerClient producerClient;

// Create a producer client that you can use to send events to an event hub
producerClient = new EventHubProducerClient(connectionString, eventHubName);


// Create a schema registry client that you can use to serialize and validate data.  
var schemaRegistryClient = new SchemaRegistryClient(schemaRegistryEndpoint, new DefaultAzureCredential());

// Create an Avro object serializer using the Schema Registry client object. 
var serializer = new SchemaRegistryAvroSerializer(schemaRegistryClient, schemaGroup, new SchemaRegistryAvroSerializerOptions { AutoRegisterSchemas = true });

// Create a new order object using the generated type/class 'Order'. 
var sampleOrder = new Order { id = "1234", amount = 45.29, description = "First sample order." };
EventData eventData = (EventData)await serializer.SerializeAsync(sampleOrder, messageType: typeof(EventData));

// Create a batch of events 
using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

// Add the event data to the event batch. 
eventBatch.TryAdd(eventData);

// Send the batch of events to the event hub. 
await producerClient.SendAsync(eventBatch);
Console.WriteLine("A batch of 1 order has been published.");

