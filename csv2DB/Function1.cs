// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Microsoft.Data.Analysis;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Threading.Tasks;
using System.IO;
using System.Text;
using System.Runtime.Serialization;
using System.Collections.Generic;
using System.Runtime.Serialization.Json;

namespace csv2DB
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static void Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            const string connectionString = "";
            var tsk = Task.Run(async () =>
            {
                var storageAccount = new BlobServiceClient(connectionString);
                var containerClient = storageAccount.GetBlobContainerClient("sample");
                var eventGridEventData = getObjectFromJson(eventGridEvent.Data.ToString(), typeof(EventGridEventData));


                string blobName = System.IO.Path.GetFileName(eventGridEventData.blobUrl);

                var blobClient = containerClient.GetBlobClient(blobName);
                BlobDownloadInfo download = await blobClient.DownloadAsync();
                using (var ms = new MemoryStream())
                {
                    await download.Content.CopyToAsync(ms);
                    ms.Seek(0, SeekOrigin.Begin);
                    using (var sr = new StreamReader(ms, Encoding.GetEncoding("UTF-8")))
                    {
                        var df = DataFrame.LoadCsv(ms);
                        log.LogInformation($"Rows Count={df.Rows.Count}"); 

                    }
                }
            });
            tsk.Wait();
            //

            log.LogInformation(eventGridEvent.Data.ToString());

            EventGridEventData getObjectFromJson(string jsonString, Type t)
            {
                var serializer = new DataContractJsonSerializer(t);
                var jsonBytes = Encoding.Unicode.GetBytes(jsonString);
                var sr = new MemoryStream(jsonBytes);
                return (EventGridEventData)serializer.ReadObject(sr);
            }
        }
        [DataContract]
        public class EventGridEventData
        {
            [DataMember]
            public string api { get; set; }
            [DataMember]
            public string clientRequestId { get; set; }
            [DataMember]
            public string requestId { get; set; }
            [DataMember]
            public string eTag { get; set; }
            [DataMember]
            public string contentType { get; set; }
            [DataMember]
            public int contentLength { get; set; }
            [DataMember]
            public string blobType { get; set; }
            [DataMember]
            public string blobUrl { get; set; }
            [DataMember]
            public string url { get; set; }
            [DataMember]
            public string sequencer { get; set; }
            [DataMember]
            public IDictionary<string, string> storageDiagnostics { get; set; }
        }
    }
}
