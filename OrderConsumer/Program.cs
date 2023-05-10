using Azure.Data.SchemaRegistry;
using Azure.Identity;
using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;
using Azure.Storage.Blobs;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using com.azure.schemaregistry.samples;

// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");

// connection string to the Event Hubs namespace
const string connectionString = "Endpoint=sb://hvtien3eventhubns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DB2bfCPNJHHzJJIyjAjpHntmo6Oiu+zsh+AEhAlUAFs=";

// name of the event hub
const string eventHubName = "hvtien3eventhub";

// Schema Registry endpoint 
const string schemaRegistryEndpoint = "hvtien3eventhubns.servicebus.windows.net";

// name of the consumer group   
const string schemaGroup = "hvtien3schemagroup";

// connection string for the Azure Storage account
const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=sahvtien3lab;AccountKey=H+JbVZjvTmIWCxkDkcOVnssNHcPPXUvmMRztaW4M42BOhEGTKhoQmWh9n9szDL/JZC20nllci5VS+AStU6Nhkw==;EndpointSuffix=core.windows.net";

// name of the blob container that will be userd as a checkpoint store
const string blobContainerName = "democontainer1";

// Create a blob container client that the event processor will use 
BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

// Create an event processor client to process events in the event hub
EventProcessorClient processor = new EventProcessorClient(storageClient, EventHubConsumerClient.DefaultConsumerGroupName, connectionString, eventHubName);

// Register handlers for processing events and handling errors
processor.ProcessEventAsync += ProcessEventHandler;
processor.ProcessErrorAsync += ProcessErrorHandler;

// Start the processing
await processor.StartProcessingAsync();

// Wait for 30 seconds for the events to be processed
await Task.Delay(TimeSpan.FromSeconds(30));

// Stop the processing
await processor.StopProcessingAsync();

static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
{
    // Create a schema registry client that you can use to serialize and validate data.  
    var schemaRegistryClient = new SchemaRegistryClient(schemaRegistryEndpoint, new DefaultAzureCredential());

    // Create an Avro object serializer using the Schema Registry client object. 
    var serializer = new SchemaRegistryAvroSerializer(schemaRegistryClient, schemaGroup, new SchemaRegistryAvroSerializerOptions { AutoRegisterSchemas = true });

    // Deserialized data in the received event using the schema 
    Order sampleOrder = (Order)await serializer.DeserializeAsync(eventArgs.Data, typeof(Order));

    // Print the received event
    Console.WriteLine($"Received order with ID: {sampleOrder.id}, amount: {sampleOrder.amount}, description: {sampleOrder.description}");

       await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
    }

    static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
{
    // Write details about the error to the console window
    Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
    Console.WriteLine(eventArgs.Exception.Message);
    return Task.CompletedTask;
}      