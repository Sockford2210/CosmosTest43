using System;
using Newtonsoft.Json;

namespace OF.Nexus.CosmosTest
{
    public class Document 
    {
        [JsonProperty(PropertyName = "id")]
        public String id {get; set;}
        public String spUrl {get; set;}
        public String timeStamp {get; set;}
        public dynamic metadata {get; set;}
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}