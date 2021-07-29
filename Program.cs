using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;


namespace EventHubsDataLake
{
    class Program
    {
        private static IConfigurationRoot Configuration;

        static void Main(string[] args)
        {
            BootstrapConfiguration();
            
            // Obtain Connection String from appsettings.json or user-secrets (secrets.json)
            var blobConnString = Configuration["SecretStrings:BlobConnectionString"]; 
            var ehConnString = Configuration["SecretStrings:EhConnectionString"];

            //Create an Event Hub Handler Class for use in this function
            EventHubHandler ehh = new EventHubHandler(ehConnString);

            //--------------------------------------------------
            //Add Something to an Event Hub Queue
            //--------------------------------------------------
            Console.WriteLine("Sending 3 sample JSON messages to EventHub");
            //Calling an async method requires we get an awaiter inside of main() if we want the results
            for (int i = 0; i < 3; i++)
            {
                string produceResult = ehh.produceEvent("Sample" + i).GetAwaiter().GetResult();
                Console.WriteLine(produceResult);
            }
            //--------------------------------------------------
            //Read Just the new messages on the Event Hub Queue
            //--------------------------------------------------
            Console.WriteLine("\n");
            Console.WriteLine("Retrieve only new un-processed messages sent to EventHub");
            //Calling an async method requires we get an awaiter inside of main() if we want the results
            string processResult = ehh.processNewEvents(blobConnString).GetAwaiter().GetResult();
            Console.WriteLine(processResult);
            Console.WriteLine("\n");

            //--------------------------------------------------
            //Read all the messages from the past hour
            //--------------------------------------------------
            Console.WriteLine("\n");
            Console.WriteLine("Retrieve all messages sitting in EventHub (max of 100)");
            string processAllResult = ehh.processAllEvents().GetAwaiter().GetResult();
            Console.WriteLine(processAllResult);
            Console.WriteLine("\n");


        }

        // ===============================================================================================
        // This Boot-strap section is needed for Local Development Settings
        // Used to protect secrets on local machines not checked into code, like connection strings.
        // https://docs.microsoft.com/en-us/aspnet/core/security/app-secrets?view=aspnetcore-5.0
        // ===============================================================================================
        private static void BootstrapConfiguration()
        {
            string env = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            var isDevelopment = string.IsNullOrEmpty(env) ||  env.ToLower() == "development";

            var builder = new ConfigurationBuilder();

            //set the appsettings file for general configuration (stuff that's not secret)
            builder.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            //if development, load user secrets as well
            if (isDevelopment)
            {
                builder.AddUserSecrets<Program>();
            }

            Configuration = builder.Build();
            return;
            // ===============================================================
        }
    }
}
