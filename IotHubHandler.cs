using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs.Consumer;
using Microsoft.Azure.Devices.Client;

namespace EventHubsDataLake
{
    public class IotHubHandler
    {
        private string _ehConnString;
        private string _iotConnString;
        
        public IotHubHandler(string szIotConnString, string szEhConnString)
        {
            _ehConnString = szEhConnString;
            _iotConnString = szIotConnString;
        }

        public async Task<string> produceEvent(string szName)
        {
            // Specify options (such as Model number) here.  For more information, see docs here:
            // https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.devices.client.clientoptions
            var options = new ClientOptions
            {
                ModelId = "xmlclassroom:com:example:TelemetryExample;1", //Specify the devices' model number here. Fake one provided
            };

            try
            {
                // Create a device with the connction string and options above, specifying MQTT as the transport.
                // https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-mqtt-support 
                DeviceClient deviceClient = DeviceClient.CreateFromConnectionString(_iotConnString, TransportType.Mqtt, options);
                
                // Create a sample message to send
                SampleSqlData sample = new SampleSqlData(szName, DateTime.Now);

                // Convert the JSON into a Message (the format required for SendEventAsync) which requires a byte array 
                Message message = new Message(Encoding.UTF8.GetBytes(sample.ToString()));

                // Send the message
                await deviceClient.SendEventAsync(message);

                return "Sent Message to IoT Hub";
            }
            catch (System.Exception ex)
            {
                // in real life different excetpion types would be caught 
                // and handled differently.
                return "Error: " + ex.Message.ToString();
            }
            
            

        }

        // This method uses ReadEventsAsync() to obtain all records within the past hour.
        // As it looks at a single partition, will loop through all partitions to get all events 
        // regardless of partition.  Based upon example provided here:
        // https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/eventhub/Azure.Messaging.EventHubs/samples/Sample05_ReadingEvents.md#read-events-from-all-partitions
        public async Task<string> processAllEvents()
        {
            var consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            var consumer = new EventHubConsumerClient(
                consumerGroup,
                _ehConnString);

            try
            {
                using CancellationTokenSource cancellationSource = new CancellationTokenSource();
                //Cancel after 5 seconds or 100 events (whichever comes first)
                cancellationSource.CancelAfter(TimeSpan.FromSeconds(5));
                int eventsRead = 0;
                int maximumEvents = 100;

                await foreach (PartitionEvent partitionEvent in consumer.ReadEventsAsync(cancellationSource.Token))
                {
                    string readFromPartition = partitionEvent.Partition.PartitionId;
                    //byte[] eventBodyBytes = partitionEvent.Data.EventBody.ToArray();
                    Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray()));

                    //Debug.WriteLine($"Read event of length { eventBodyBytes.Length } from { readFromPartition }");
                    eventsRead++;

                    if (eventsRead >= maximumEvents)
                    {
                        break;
                    }
                }
                return "Processed";
            }
            catch (TaskCanceledException)
            {
                // This is expected if the cancellation token is
                // signaled.
                return "Canceled";
            }
            finally
            {
                await consumer.CloseAsync();
            }

        }

    }
}