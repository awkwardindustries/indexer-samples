using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NGA.OpenData.Indexer.Models;
using NGA.OpenData.Indexer.Services;
using Polly;
using Polly.Extensions.Http;

namespace NGA.OpenData.Indexer;

class Program
{
    private static IConfiguration? _config;
    private static Settings? _settings;

    static async Task Main(string[] args)
    {
        // ///////////////////
        // Setup services
        // ///////////////////

        // Configure the run
        _config = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .AddEnvironmentVariables()
            .Build();

        _settings = _config.GetRequiredSection("Settings").Get<Settings>()!;

        // Validate settings
        if (_settings is null)
            throw new ArgumentException("Settings required. Check your appsettings.json exists.");
        if (string.IsNullOrEmpty(_settings.AzureAiVisionKey) || string.IsNullOrEmpty(_settings.AzureAiVisionEndpoint))
            throw new ArgumentException("Azure AI Vision settings missing. Check your appsettings.json for 'AzureAiVisionKey' and 'AzureAiVisionEndpoint' settings.");
        if (string.IsNullOrEmpty(_settings.NgaOpenDataPostgresqlConnectionString))
            throw new ArgumentException("Source database connection string not provided. Check your appsettings.json for 'NgaOpenDataPostgresqlConnectionString' setting.");
        if (string.IsNullOrEmpty(_settings.AzureAiSearchKey) || string.IsNullOrEmpty(_settings.AzureAiSearchEndpoint))
            throw new ArgumentException("Azure AI Search settings missing. Check your appsettings.json for 'AzureAiSearchKey' and 'AzureAiSearchEndpoint' settings.");
        if (string.IsNullOrEmpty(_settings.AzureAiSearchIndexName))
            _settings.AzureAiSearchIndexName = "gallerydata";

        // Setup HTTP
        var serviceCollection = new ServiceCollection();
        serviceCollection.AddHttpClient(Constants.IMAGE_VECTORIZOR_HTTP_CLIENT)
            .AddPolicyHandler(GetRetryPolicy());
        var serviceProvider = serviceCollection.BuildServiceProvider();
        var httpClientFactory = serviceProvider.GetService<IHttpClientFactory>() ?? throw new ArgumentException("Error creating HTTP Client Factory.");

        // Setup services
        var indexManager = new IndexManager(_settings.AzureAiSearchKey, _settings.AzureAiSearchEndpoint, _settings.AzureAiSearchIndexName);
        var retriever = new OpenDataRetriever(_settings.NgaOpenDataPostgresqlConnectionString);
        var vectorizor = new ImageVectorizer(httpClientFactory, _settings.AzureAiVisionKey, _settings.AzureAiVisionEndpoint);

        Console.WriteLine("Application configured. Creating index...");

        // ///////////////////
        // Create the index
        // ///////////////////

        // Ensure Index is ready
        await indexManager.Create(_settings.DropAndRecreateIndexIfItExists);

        Console.WriteLine("Index created. Getting OpenData records...");

        // ///////////////////
        // Populate the index
        // ///////////////////

        // Get data from NGA OpenData source
        var records = await retriever.GetAllOpenDataRecords();

        Console.WriteLine("Records retrieved. Enriching with embeddings...");

        // Map to target and generate image embeddings for each record in chunks
        var indexDocuments = new List<IndexDocument>();
        var sizeOfChunk = 1000;
        var currentChunk = 0;
        var chunks = records.Chunk(sizeOfChunk);
        var numChunks = chunks.Count();

        foreach (var chunk in chunks) 
        {
            currentChunk++;
            indexDocuments.Clear();

            foreach (var record in chunk)
            {
                if (!string.IsNullOrEmpty(record.IiifUrl))
                    try 
                    {
                        var indexDocument = new IndexDocument(record)
                        {
                            VectorizedImage = await vectorizor.VectorizeImage($"{record.IiifUrl}/full/!600,600/0/default.jpg")
                        };
                        indexDocuments.Add(indexDocument);
                    } 
                    catch (Exception)
                    {
                        Console.WriteLine($"Unexpected issue getting vector data for object. Skipping: [ ID = {record.ObjectID}, Title = {record.Title}, IIIFUrl = {record.IiifUrl} ]");
                    }
                else
                    Console.WriteLine($"No IIIFUrl. Skipping: [ ID = {record.ObjectID}, Title = {record.Title}, IIIFUrl = {record.IiifUrl} ]");
            }

            Console.WriteLine($"[{currentChunk} of {numChunks}] Index documents for chunk ready. Bulk inserting...");

            // Persist to Azure AI Search
            try 
            {
                await indexManager.BulkInsert(indexDocuments);
            } 
            catch (Exception e)
            {
                Console.WriteLine($"This batch could not be saved to the index. Continuing. Error: {e.Message}");
            }
        }

        Console.WriteLine("All data processed and index updated. Goodbye.");
    }

    static IAsyncPolicy<HttpResponseMessage> GetRetryPolicy()
    {
        return HttpPolicyExtensions
            .HandleTransientHttpError()
            .OrResult(msg => msg.StatusCode == System.Net.HttpStatusCode.NotFound)
            .WaitAndRetryAsync(6, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
    }
}
