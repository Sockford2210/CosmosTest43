using System;
using System.Threading.Tasks;
using System.Threading;
using System.IO;
using System.Text;
using System.Linq;
using Microsoft.Azure.Cosmos;
using System.Collections.Generic;
using System.Diagnostics;

namespace OF.Nexus.CosmosTest
{
    public class Tester
    {
        private CosmosInterface cosmosInterface;
        private readonly string databaseName = "LargeDatasetTestDB"; 
        private readonly string containerName = "LargeDatasetTestContainer";
        private readonly string partitionKey = "/id";
        private DateTime timeStampStart;
        private int timeStampRangeDays;

        public Tester()
        {
        }

        public Tester(string cosmosUrl, string cosmosKey)
        {
            this.cosmosInterface = new CosmosInterface(cosmosUrl, cosmosKey, databaseName);
            this.timeStampStart = new DateTime(2021, 1, 1);
            this.timeStampRangeDays = (int)(DateTime.Today - this.timeStampStart).TotalDays;   
        }

        public async Task Run()
        {
            await this.cosmosInterface.CreateContainer(this.databaseName, this.containerName, this.partitionKey);
            bool exit = false;
            while(!exit)
            {
                Console.WriteLine("");
                Console.WriteLine("MENU");
                try
                {
                    int documentCount = await this.cosmosInterface.DocumentCount();
                    Console.WriteLine("Document Count: " + documentCount);
                }
                catch(Exception)
                {
                    Console.WriteLine("Document Count: NOT FOUND");
                }
                
                Console.WriteLine("Add random documents: 1");
                Console.WriteLine("Read documents with point read: 2");
                Console.WriteLine("Run SQL query on container: 3");
                Console.WriteLine("Enter: ");
                string entry = Console.ReadLine();
                switch(entry)
                {
                    case "1":
                        await this.AddDocuments();
                        break;
                    case "2":
                        await this.QueryDocumentsByPointRead();
                        break;
                    case "3":
                        await this.QueryDocuments();
                        break;
                    default:
                        exit = true;
                        break;
                }
            }
            Console.WriteLine("Terminated");
        }

        public async Task GetContainerInfo()
        {
            int documentCount = await this.cosmosInterface.DocumentCount();
            Console.WriteLine("Document Count: " + documentCount);
        }

        public async Task AddDocuments()
        {
            Console.WriteLine("Amount to add: ");
            int amountToAdd = Int32.Parse(Console.ReadLine());
            List<Document> documentsToAdd = GetDocumentsToInsert(amountToAdd);
            await this.cosmosInterface.AddBulkDocuments(documentsToAdd);
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

        private List<Document> GetDocumentsToInsert(int amountToAdd)
        {
            Random rand = new Random();
            List<Document> documentsToAdd = new List<Document>(amountToAdd);
            for(int i = 0; i < amountToAdd; i++)
            {
                string id = Guid.NewGuid().ToString();
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
                    id = id, 
                    spUrl = "/sites/dev_sp_nexusdocumentlibrary/<week>-<year>", 
                    timeStamp = this.RandomDay(), 
                    metadata = documentMetadata 
                });
            }
            return documentsToAdd;
        }

        private async Task QueryDocuments()
        {
            Console.WriteLine("Enter SQL query: ");
            string queryString = Console.ReadLine();
            CosmosResponse cosmosResponse = await this.cosmosInterface.RunQueryAsync(queryString);
            List<CosmosResponse> cosmosResponseList = new List<CosmosResponse>{ cosmosResponse };
            this.CsvWriter(cosmosResponseList, "Cosmos_Query_Test", 1);         
        }

        private async Task QueryDocumentsByPointRead()
        {
            List<string> idsToQuery = new List<string>();
            bool finished = false;
            while (!finished)
            { 
                Console.WriteLine("Enter id to query (leave blank to exit): ");
                string entry = Console.ReadLine();
                if (entry == "")
                {
                    finished = true;
                }
                else
                {
                    idsToQuery.Add(entry);
                }
            }
            
            List<CosmosResponse> responses = new List<CosmosResponse>();
            foreach(string id in idsToQuery)
            {
                responses.Add(await this.cosmosInterface.ReadDocumentAsync(id));
            }

            int documentCount = await this.cosmosInterface.DocumentCount();        
            this.CsvWriter(responses, "Cosmos_Read_By_Id_Test", responses.Count);
        }

        private void CsvWriter(List<CosmosResponse> cosmosResponses, String fileName, int trialNumber)
        {
            string filePath = String.Format(@"H:\sys-sharepoint\CosmosTest1655\{0}.csv",fileName);
            if (!File.Exists(filePath))
            {
                string firstline = ",Time Taken(ms)" + String.Concat(Enumerable.Repeat(",", trialNumber)) + "Request Charge (RU)";
                IEnumerable<int> trialRange = Enumerable.Range(1, trialNumber);
                string trialString = "," + String.Join(",",trialRange);
                string secondline = "Total Documents" + String.Concat(Enumerable.Repeat(trialString, 2));
                File.WriteAllText(filePath,String.Format("{0}\r\n{1}\r\n",firstline, secondline));
            }

            int documentCount = cosmosResponses[0].documentCount;
            StringBuilder responseDetailsLine = new StringBuilder();
            responseDetailsLine.Append(documentCount.ToString());
            string readTimeString = "";
            string readRequestChargeString = "";
            foreach(CosmosResponse response in cosmosResponses){
                readTimeString += String.Format(",{0}", response.timeElapsed); 
                readRequestChargeString += String.Format(",{0}", response.requestUnitsUsed);
            }
            responseDetailsLine.AppendLine(readTimeString + readRequestChargeString);
            bool success = false;
            while (!success)
            {
                try
                {
                    File.AppendAllText(filePath, responseDetailsLine.ToString());
                    success = true;
                }
                catch (System.IO.IOException)
                {
                    Console.WriteLine(string.Format("The file: {0} is in use, please close it and press any key to try again.", filePath));
                    Console.ReadKey();
                }
            }      
        }
    }
}