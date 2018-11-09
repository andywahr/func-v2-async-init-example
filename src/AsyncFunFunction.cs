using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Nito.AsyncEx;
using System.Collections.Generic;
using System.Threading;
using System.Reflection;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Documents;
using System.Linq;
using Microsoft.Azure.Documents.Linq;
using System.Text;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ApplicationInsights.DataContracts;

namespace SampleFunction
{
    public static class AsyncFunFunction
    {
        static System.Timers.Timer CacheTimer;

        static AsyncFunFunction()
        {
            // 1. Run a synchronous method at startup one time per Functions App instance
            StartupSetup();

            // 2. Run an asynchronous method at startup one time per Functions App instance
            StartupSetupAsync().Wait();

            // 3. Run an asynchronous method at startup one time per Functions App instance and get a return value
            OtherStuff = StartupSetupWithResultAsync().Result;

            CacheTimer = new System.Timers.Timer();
            CacheTimer.Elapsed += CacheTimer_Elapsed;
            CacheTimer.Interval = TimeSpan.FromMinutes(1).TotalMilliseconds;
            CacheTimer.Enabled = true;

            // Init CosmosDB 
            IConfigurationRoot configuration = new ConfigurationBuilder()
                 .SetBasePath(Environment.CurrentDirectory)
                 .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                 .AddEnvironmentVariables().Build();

            CosmosDbName = configuration["CosmosDbName"];
            CosmosDbCollectionName = configuration["CosmosDbCollectionName"];

            DocumentClient = new DocumentClient(
                new Uri(configuration["CosmosDbEndpoint"]),
                configuration["CosmosDbKey"],
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                }
            );
        }


        [FunctionName("AsyncFun")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req, ILogger log, CancellationToken token)
        {
            // 4. Run an asynchronous method during first function invocation per Functions App instance, blocking other calls until complete
            bool verifyInitialized = await DoInitialization;

            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            // 5. Run an asynchronous method during first function invocation per Functions App instance with a return value, blocking other calls until complete
            Dictionary<int, string> stuffINeed = await InitializeMyStuff;

            // 6. Manage a static/single instance resource, ensuring only 1 call is doing the load/reloading at a time per Function App instance. 
            Dictionary<int, string> myLocalCacheRef = await GetCacheRef();

            // Rosyln Example to do dynamic code 
            string rulesOutput = await RunRules(name);

            // Cosmos DB Example to minimize Serialization
            JArray listOfCars = await ExecuteCosmosQuery();

            return name != null
                ? (ActionResult)new OkObjectResult($"Hello, {rulesOutput}")
                : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
        }

        #region 1. Run a synchronous method at startup one time per Functions App instance
        static void StartupSetup()
        {
            System.Diagnostics.Trace.WriteLine($"{DateTime.Now.ToString("yyyyMMddHHmmss")}: 1. Running StartupSetup");
        }
        #endregion

        #region 2. Run an asynchronous method at startup one time per Functions App instance
        static async Task StartupSetupAsync()
        {
            System.Diagnostics.Trace.WriteLine($"{DateTime.Now.ToString("yyyyMMddHHmmss")}: 2. Running StartupSetupAsync");
            await Task.Delay(0);
        }
        #endregion

        #region 3. Run an asynchronous method at startup one time per Functions App instance and get a return value
        static Dictionary<int, string> OtherStuff = null;

        static async Task<Dictionary<int, string>> StartupSetupWithResultAsync()
        {
            System.Diagnostics.Trace.WriteLine($"{DateTime.Now.ToString("yyyyMMddHHmmss")}: 3. Running StartupSetupWithResultAsync");

            Dictionary<int, string> myStuff = new Dictionary<int, string>();

            for (int ii = 0; ii < 100; ii++)
            {
                myStuff.Add(ii, ii.ToString());
            }

            return await Task.FromResult(myStuff);
        }
        #endregion

        #region 4. Run an asynchronous method during first function invocation per Functions App instance, blocking other calls until complete
        static AsyncLazy<bool> DoInitialization = new AsyncLazy<bool>(InitializeMe);
        static async Task<bool> InitializeMe()
        {
            System.Diagnostics.Trace.WriteLine($"{DateTime.Now.ToString("yyyyMMddHHmmss")}: 4. Running InitializeMe");
            return await Task.FromResult(true);
        }
        #endregion

        #region 5. Run an asynchronous method during first function invocation per Functions App instance with a return value, blocking other calls until complete
        static AsyncLazy<Dictionary<int, string>> InitializeMyStuff = new AsyncLazy<Dictionary<int, string>>(LoadStuff);
        static async Task<Dictionary<int, string>> LoadStuff()
        {
            System.Diagnostics.Trace.WriteLine($"{DateTime.Now.ToString("yyyyMMddHHmmss")}: 5. Running LoadStuff");

            Dictionary<int, string> myStuff = new Dictionary<int, string>();

            for (int ii = 0; ii < 100; ii++)
            {
                myStuff.Add(ii, ii.ToString());
            }

            return await Task.FromResult(myStuff);
        }
        #endregion

        #region 6. Manage a static/single instance resource, ensuring only 1 call is doing the load/reloading at a time per Function App instance. 
        static AsyncLazy<bool> FirstTimeCache = new AsyncLazy<bool>(LoadMyCache);
        static Dictionary<int, string> CacheMyStuff = null;
        static AsyncReaderWriterLock RWL = new AsyncReaderWriterLock();
        static DateTimeOffset TimeCached = DateTimeOffset.MinValue;
        static TimeSpan TimeToLive = TimeSpan.FromMinutes(1);
        static async Task<Dictionary<int, string>> GetCacheRef()
        {
            // Ensure Cache is loaded first time, all subsequent calls will just basically do a No-Op
            await FirstTimeCache;

            System.Diagnostics.Trace.WriteLine($"{DateTime.Now.ToString("yyyyMMddHHmmss")}: Running GetCacheRef");

            using (var readerLockToken = await RWL.ReaderLockAsync())
            {
                return CacheMyStuff;
            }
        }

        static async Task<bool> LoadMyCache()
        {
            return await TrackDependency(async () =>
            {
                System.Diagnostics.Trace.WriteLine($"{DateTime.Now.ToString("yyyyMMddHHmmss")}: 6. Running LoadMyCache");

                Dictionary<int, string> myStuff = new Dictionary<int, string>();

                for (int ii = 0; ii < 100; ii++)
                {
                    myStuff.Add(ii, ii.ToString());
                }

                using (var writerLockToken = await RWL.WriterLockAsync())
                {
                    CacheMyStuff = myStuff;
                    TimeCached = DateTimeOffset.Now;

                }
            }, "Cache", "Reload", "Background", false);
        }

        static void CacheTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (!LoadMyCache().Result )
            {
                // Do something
            }
        }

        #endregion

        #region Cosmos DB Example to minimize Serialization

        private static DocumentClient DocumentClient;
        private static string CosmosDbName;
        private static string CosmosDbCollectionName;

        public static async Task<List<T>> ToListAsync<T>(this IDocumentQuery<T> queryable)
        {
            var list = new List<T>();
            while (queryable.HasMoreResults)
            {   //Note that ExecuteNextAsync can return many records in each call
                var response = await queryable.ExecuteNextAsync<T>();
                list.AddRange(response);
            }
            return list;
        }

        public static async Task<List<T>> ToListAsync<T>(this IQueryable<T> query)
        {
            return await query.AsDocumentQuery().ToListAsync();
        }

        private static async Task<JArray> ExecuteCosmosQuery()
        {
            List<Document> documents = null;
            JArray retVal = null;

            await TrackDependency(async () =>
                                            {
                                                var query = DocumentClient.CreateDocumentQuery<Document>(
                                                                UriFactory.CreateDocumentCollectionUri(CosmosDbName, CosmosDbCollectionName),
                                                                new SqlQuerySpec()
                                                                {
                                                                    QueryText = "SELECT * FROM Cars c",
                                                                },
                                                                new FeedOptions { EnableCrossPartitionQuery = true });



                                                documents = await query.ToListAsync();
                                            }, "CosmoDB", "Query", "Cars");

            if (documents == null || !documents.Any())
            {
                return JArray.Parse("[]");
            }

            await TrackDependency(async () =>
            {
                StringBuilder sb = new StringBuilder("[");
                sb.Append(documents.First().ToString());

                foreach (var doc in documents.Skip(1))
                {
                    sb.Append(',');
                    sb.Append(doc.ToString());
                }

                sb.Append(']');

                retVal = JArray.Parse(sb.ToString());
                await Task.Delay(0);
            }, "Function", "GetResults", "Deserialization");

            return retVal;

        }
        #endregion

        #region Rosyln Example to do dynamic code 
        private static async Task<string> RunRules(string name)
        {
            var func = await CSharpScript.RunAsync<Func<string, string>>("new Func<string, string>(input => input.ToUpper())",
                                                                            Microsoft.CodeAnalysis.Scripting.ScriptOptions.Default.
                                                                            WithReferences(Assembly.GetCallingAssembly(), Assembly.GetExecutingAssembly()).
                                                                            WithImports("System", "System.Text"));

            return func.ReturnValue(name);
        }
        #endregion

        #region Custom Dependency Tracking
        private static async Task<bool> TrackDependency(Func<Task> func, string type, string action, string detail, bool rethrow = true)
        {
            TelemetryClient telemetryClient = new TelemetryClient(TelemetryConfiguration.Active);
            var operation = telemetryClient.StartOperation<DependencyTelemetry>($"{action} {detail}");
            operation.Telemetry.Type = type;
            operation.Telemetry.Data = $"{action} {detail}";

            try
            {
                await func();

                // Set operation.Telemetry Success and ResponseCode here.
                operation.Telemetry.Success = true;
                return true;
            }
            catch (Exception e)
            {
                telemetryClient.TrackException(e);
                // Set operation.Telemetry Success and ResponseCode here.
                operation.Telemetry.Success = false;
                if (rethrow)
                {
                    throw;
                }
                return false;
            }
            finally
            {
                telemetryClient.StopOperation(operation);
            }
        }
        #endregion
    }
}
