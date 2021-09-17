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
        private readonly string databaseName = "FunctionTestDatabase"; 
        private readonly string containerName = "DocRefContainer";
        private readonly string partitionKey = "/documentRef";

        public Tester()
        {
        }

        public Tester(string cosmosUrl, string cosmosKey)
        {
            this.cosmosInterface = new CosmosInterface(cosmosUrl, cosmosKey, databaseName);  
        }

        public async Task Run()
        {
            await this.cosmosInterface.CreateContainer(this.databaseName, this.containerName, this.partitionKey);
            bool exit = false;
            while(!exit)
            {
                Console.WriteLine("");
                Console.WriteLine("MENU");

                int documentCount = await this.cosmosInterface.DocumentCount();
                Console.WriteLine("Document Count: " + documentCount);              
                
                Console.WriteLine("Add random documents: 1");
                Console.WriteLine("Read documents with point read: 2");
                Console.WriteLine("Run SQL query on container: 3");
                Console.WriteLine("Execute stored procedure: 4");
                Console.Write("Enter: ");
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
                    case "4":
                        await this.RunStoredProcedure();
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
            Console.Write("Amount to add: ");
            int amountToAdd = Int32.Parse(Console.ReadLine());
            await this.cosmosInterface.AddBulkDocuments(amountToAdd);
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

        private async Task RunStoredProcedure()
        {
            Console.Write("Stored Procedure Id: ");
            string storedProcedureId = Console.ReadLine();
            Console.Write("Parameter in JSON: ");
            string jsonObjectParam = Console.ReadLine();
            Console.WriteLine("Executing stored procedure");
            CosmosResponse cosmosResponse = await this.cosmosInterface.ExecuteStoredProcedureAsync(storedProcedureId, jsonObjectParam);
            if(cosmosResponse.success)
            {
                Console.WriteLine("Great Success");
            }
            else{
                Console.WriteLine("Much sad");
            }
        }

        private void CsvWriter(List<CosmosResponse> cosmosResponses, String fileName, int trialNumber)
        {
            string filePath = String.Format(@".\{0}.csv",fileName);
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