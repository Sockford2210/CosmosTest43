using System;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace OF.Nexus.CosmosTest
{
    public class CosmosResponse 
    {
        public List<Document> documents {get; set;}
        public long timeElapsed {get; set;}
        public double requestUnitsUsed {get; set;}
        public int documentCount {get; set;}
        public bool success {get; set;}
    }
}