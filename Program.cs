using System;

namespace OF.Nexus.CosmosTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var cosmosUrl = "https://cosmos-test-43.documents.azure.com:443/";
            var cosmosKey = "xaLy813hOIbdFDpf5H5iDzgwFbbrzDR04RxPNcTqqAGqs2DzFOLo3JFfweUdHL9dKyc8gBQsLCDsCeX56YMfBw==";
            Tester cosmosTester = new Tester(cosmosUrl, cosmosKey);
            cosmosTester.Run().Wait();
        }
    }
}
