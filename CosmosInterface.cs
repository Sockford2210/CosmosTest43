using System;
using System.IO;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Diagnostics;

namespace OF.Nexus.CosmosTest
{
    public class CosmosInterface
    {
        public CosmosClient client { get; private set; }
        public Database database { get; private set; }
        public Container container { get; private set; }

        public CosmosInterface(string cosmosUrl, string cosmosKey, string databaseName)
        {
            try
            {
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
                string storedProcedureId = "spBulkImport";
                StoredProcedureResponse storedProcedureResponse = await this.container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties
                {
                    Id = storedProcedureId,
                    Body = File.ReadAllText($@".\js\{storedProcedureId}.js")
                });
                Console.WriteLine(String.Format("Container: {0} created in database: {1}", containerName, databaseName));
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
                var response = await query.ReadNextAsync();
                return response.Resource.GetEnumerator().Current;
            }
            catch(Microsoft.Azure.Cosmos.CosmosException ex)
            {
                Console.WriteLine("Cosmos returned error: " + ex.ToString());
                return -1;
            }           
        }

        public async Task AddBulkDocuments(List<Document> documents)
        {
            List<Task> concurrentTasks = new List<Task>();
            List<CosmosResponse> cosmosResponses = new List<CosmosResponse>();
            int index = 0;
            foreach(Document document in documents)
            {
                if(index >= 5000 && (index % 5000 == 0))
                {
                    await this.RunConcurrentDocumentInserts(concurrentTasks);
                    concurrentTasks.Clear();
                }
                else
                {
                    concurrentTasks.Add(container.CreateItemAsync(documents[index], new PartitionKey(documents[i].id)));
                    index ++;
                }
            }

        }

        private async Task RunConcurrentDocumentInserts(List<Task> concurrentTasks)
        {
            try
            {
                int numberToAdd = concurrentTasks.Count;
                Stopwatch stopwatch = Stopwatch.StartNew();
                await Task.WhenAll(concurrentTasks);
                stopwatch.Stop();
                Console.WriteLine(String.Format("{0} documents added, took {1}ms", numberToAdd, stopwatch.ElapsedMilliseconds));
            }
            catch (Microsoft.Azure.Cosmos.CosmosException ex)
            {
                Console.WriteLine("Cosmos returned error: " + ex.ToString());
                return;
            }
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
    }
}