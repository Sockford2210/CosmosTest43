using System;
using System.IO;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;
using Newtonsoft.Json.Linq;

namespace OF.Nexus.CosmosTest
{
    public class CosmosInterface
    {
        public CosmosClient client { get; private set; }
        public Database database { get; private set; }
        public Container container { get; private set; }
        private DateTime timeStampStart;
        private int timeStampRangeDays;

        public CosmosInterface(string cosmosUrl, string cosmosKey, string databaseName)
        {
            try
            {
                this.timeStampStart = new DateTime(2021, 1, 1);
                this.timeStampRangeDays = (int)(DateTime.Today - this.timeStampStart).TotalDays; 
                CosmosClientOptions clientOptions = new CosmosClientOptions() { AllowBulkExecution = true };
                this.client = new CosmosClient(cosmosUrl, cosmosKey, clientOptions);
            }
            catch (System.Exception ex)
            {            
                Console.WriteLine("Error occurred when creating CosmosInterface: " + ex.ToString());
            }
        }

        public async Task CreateContainer(string databaseName, string containerName, string partitionKey)
        {
            try
            {
                this.database = await client.CreateDatabaseIfNotExistsAsync(databaseName);
                this.container = await database.CreateContainerIfNotExistsAsync(containerName, partitionKey);
                
                string storedProcedureId = "spInsertDocument";
                StoredProcedureResponse storedProcedureResponse = await this.container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
                {
                    Id = storedProcedureId,
                    Body = File.ReadAllText($@".\js\{storedProcedureId}.js")
                });
                Console.WriteLine("Connected");
                Console.WriteLine($"Database: {databaseName}");
                Console.WriteLine($"Container: {containerName}");
            }
            catch (System.Exception ex)
            {            
                Console.WriteLine("Error occurred when creating Cosmos container: " + ex.ToString());
            }
        }

        public async Task<int> DocumentCount()
        {
            string queryString = "SELECT VALUE COUNT(1) FROM docs";
            try
            {
                var query = this.container.GetItemQueryIterator<int>(new QueryDefinition(queryString));
                List<int> results = new List<int>();
                while (query.HasMoreResults)
                {
                    var response = await query.ReadNextAsync();
                    results.AddRange(response);
                }
                int documentCount = -1;
                foreach (int result in results){
                    Console.WriteLine(String.Format("Total number of documents in container: {0}", result));
                    documentCount = result;
                }
                return documentCount;     
            }
            catch(Microsoft.Azure.Cosmos.CosmosException ex)
            {
                Console.WriteLine("Cosmos returned error: " + ex.ToString());
                return -1;
            }         
        }

        public async Task AddBulkDocuments(int amountToAdd)
        {
            List<Document> documents = GetDocumentsToInsert(amountToAdd);
            List<Task<CosmosResponse>> concurrentTasks = new List<Task<CosmosResponse>>();
            List<CosmosResponse> cosmosResponses = new List<CosmosResponse>();
            int index = 0;
            int sucessfulInserts = 0;
            Stopwatch stopwatch = Stopwatch.StartNew();
            foreach(Document document in documents)
            {
                if(index >= 20000)
                {
                    var cosmosResponse = await this.RunConcurrentDocumentInserts(concurrentTasks);
                    sucessfulInserts += cosmosResponse.documents.Count;
                    concurrentTasks.Clear();
                    index = 0;
                }
                else
                {
                    concurrentTasks.Add(AddDocumentAsync(document));
                    index ++;
                }
            }

            if(concurrentTasks.Count > 0)
            {
                var cosmosResponse = await this.RunConcurrentDocumentInserts(concurrentTasks);
                sucessfulInserts += cosmosResponse.documents.Count;
                concurrentTasks.Clear();
            }

            stopwatch.Stop();
            Console.WriteLine(String.Format("{0} documents successfully inserted, took {1}ms", sucessfulInserts, stopwatch.ElapsedMilliseconds));
            if(sucessfulInserts < amountToAdd){
                int remaining = amountToAdd - sucessfulInserts;
                Console.WriteLine(String.Format("{0} documents failed insertion, Reattempting bulk document insertion with {1} documents", remaining, remaining));
                await this.AddBulkDocuments(remaining);
            }
        }

        private async Task<CosmosResponse> RunConcurrentDocumentInserts(List<Task<CosmosResponse>> concurrentTasks)
        {
            CosmosResponse successfulResponse = new CosmosResponse();
            successfulResponse.documents = new List<Document>();
            successfulResponse.requestUnitsUsed = 0;
            try
            {
                int numberToAdd = concurrentTasks.Count;
                Stopwatch stopwatch = Stopwatch.StartNew();
                var responses = await Task.WhenAll(concurrentTasks);
                stopwatch.Stop();
                List<CosmosResponse> cosmosResponses = new List<CosmosResponse>(responses);
                successfulResponse.timeElapsed = stopwatch.ElapsedMilliseconds;
                foreach (CosmosResponse cosmosResponse in cosmosResponses)
                {
                    if (cosmosResponse.success){
                        successfulResponse.documents.Add(cosmosResponse.documents[0]);
                        successfulResponse.requestUnitsUsed += cosmosResponse.requestUnitsUsed;
                    }
                }
                int successfulDocumentCount = successfulResponse.documents.Count;
                long timeElapsed = successfulResponse.timeElapsed;
                double totalRequestUnits = successfulResponse.requestUnitsUsed;
                double requestUnitsPerSecond = totalRequestUnits/timeElapsed;
                Console.WriteLine(String.Format("Batch of {0} documents added, took {1}ms and consumed a total of {2}RUs, and {3}RU/s", successfulDocumentCount, timeElapsed, totalRequestUnits, requestUnitsPerSecond ));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.ToString());
            }

            Thread.Sleep(5000);
            return successfulResponse;
        }

        private async Task<CosmosResponse> AddDocumentAsync(Document document)
        {
            CosmosResponse cosmosResponse = new CosmosResponse();
            cosmosResponse.documents = new List<Document>(){
                document
            };
            try {
                var response = await this.container.CreateItemAsync(document, new PartitionKey(document.documentRef));
                cosmosResponse.requestUnitsUsed = response.RequestCharge;
                cosmosResponse.success = true;
            }
            catch (Microsoft.Azure.Cosmos.CosmosException)
            {
                cosmosResponse.success = false;
            }
            return cosmosResponse;           
        }

        public async Task<CosmosResponse> ReadDocumentAsync(String id)
        {
            CosmosResponse cosmosResponse = new CosmosResponse();
            try{
                Stopwatch stopwatch = Stopwatch.StartNew();
                var response = await this.container.ReadItemAsync<Document>(
                    partitionKey: new PartitionKey(id),
                    id: id
                );
                stopwatch.Stop();
                cosmosResponse.documents = new List<Document>();
                cosmosResponse.documents.Add(response.Resource);
                cosmosResponse.requestUnitsUsed = response.RequestCharge;
                cosmosResponse.timeElapsed = stopwatch.ElapsedMilliseconds;
                cosmosResponse.documentCount = await this.DocumentCount();
                return cosmosResponse;
            }
            catch (Microsoft.Azure.Cosmos.CosmosException ex)
            {
                Console.WriteLine("Cosmos returned error: " + ex.ToString());
                return cosmosResponse;
            }
        }

        public async Task<CosmosResponse> RunQueryAsync(string queryString)
        {
            CosmosResponse cosmosResponse = new CosmosResponse();
            Stopwatch stopwatch = Stopwatch.StartNew();
            try
            {
                var query = this.container.GetItemQueryIterator<Document>(new QueryDefinition(queryString));
                stopwatch.Stop();
                cosmosResponse.documents = new List<Document>();
                while (query.HasMoreResults)
                {
                    var response = await query.ReadNextAsync();
                    cosmosResponse.requestUnitsUsed = response.RequestCharge;
                    cosmosResponse.documents.AddRange(response.Resource);
                }
                cosmosResponse.timeElapsed = stopwatch.ElapsedMilliseconds;
                cosmosResponse.documentCount = await this.DocumentCount();
                return cosmosResponse;
            }
            catch (Microsoft.Azure.Cosmos.CosmosException ex)
            {
                Console.WriteLine("Cosmos returned error: " + ex.ToString());
                return cosmosResponse;
            }
        }

        public async Task<CosmosResponse> ExecuteStoredProcedureAsync(string storedProcedureId, dynamic jsonObjectParam)
        {
            CosmosResponse cosmosResponse = new CosmosResponse();
            Document document = JObject.Parse(jsonObjectParam);
            cosmosResponse.documents = new List<Document>(){ document };
            try{
                var result = await this.container.Scripts.ExecuteStoredProcedureAsync<string>(storedProcedureId, new PartitionKey(jsonObjectParam.id), new[] { jsonObjectParam });
                Console.WriteLine("Response: " + result.ToString());
                cosmosResponse.success = true;
            }
            catch (Microsoft.Azure.Cosmos.CosmosException ex)
            {
                Console.WriteLine("Cosmos returned error: " + ex.ToString());
                cosmosResponse.success = false;
            }
            return cosmosResponse;
        }

        private List<Document> GetDocumentsToInsert(int amountToAdd)
        {
            Random rand = new Random();
            List<Document> documentsToAdd = new List<Document>(amountToAdd);
            for(int i = 0; i < amountToAdd; i++)
            {
                string documentRef = Guid.NewGuid().ToString();
                string customerRef = "FCI" + (rand.Next(100000,100040)).ToString();
                string policyRef = (rand.Next(123400,123430)).ToString();
                dynamic documentMetadata = new 
                { 
                    documentClass = "SSG New Business",
                    customerRef = customerRef,
                    policyRef = policyRef,
                    mimeType = "application/pdf",
                    otherField = "example1"
                };
                documentsToAdd.Add(new Document 
                { 
                    documentRef = documentRef, 
                    spUrl = "/sites/dev_sp_nexusdocumentlibrary/<week>-<year>", 
                    timeStamp = this.RandomDay(), 
                    metadata = documentMetadata 
                });
            }

            return documentsToAdd;
        }

        private String RandomDay()
        {  
            Random rand = new Random();
            DateTime dateTime = this.timeStampStart.AddDays(rand.Next(this.timeStampRangeDays));
            dateTime = dateTime.AddHours(rand.Next(24));
            dateTime = dateTime.AddMinutes(rand.Next(60));
            dateTime = dateTime.AddSeconds(rand.Next(60));
            dateTime = dateTime.AddMilliseconds(rand.Next(1000));
            return dateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
        }
    }
}