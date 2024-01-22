using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using Azure.Search.Documents.Models;
using NGA.OpenData.Indexer.Models;

namespace NGA.OpenData.Indexer.Services;

public class IndexManager
{
    private readonly SearchIndexClient _searchIndexClient;
    private readonly SearchClient _searchClient;

    public IndexManager(string aiSearchKey, string aiSearchEndpoint, string indexName)
    {
        var credential = new AzureKeyCredential(aiSearchKey);
        var endpoint = new Uri(aiSearchEndpoint);
        _searchIndexClient = new(endpoint, credential);
        _searchClient = new(endpoint, indexName, credential);
    }

    public async Task Create(bool dropIfIndexExists)
    {
        // Drop the index if it already exists
        _searchIndexClient.GetIndexNamesAsync();
        {
            if (dropIfIndexExists)
                await _searchIndexClient.DeleteIndexAsync(_searchClient.IndexName);
        }

        // Create the index
        var searchFields = new FieldBuilder().Build(typeof(IndexDocument));
        var indexDefinition = new SearchIndex(_searchClient.IndexName)
        {
            VectorSearch = new()
            {
                Profiles = 
                {
                    new VectorSearchProfile(Constants.IMAGE_VECTOR_SEARCH_PROFILE_NAME, Constants.VECTOR_SEARCH_HNSW_CONFIG)
                },
                Algorithms =
                {
                    new HnswAlgorithmConfiguration(Constants.VECTOR_SEARCH_HNSW_CONFIG)
                }
            },
            Fields = searchFields
        };

        await _searchIndexClient.CreateIndexAsync(indexDefinition);
    }

    public async Task<Response<IndexDocumentsResult>> BulkInsert(IEnumerable<IndexDocument> indexDocuments)
    {
        return await _searchClient.UploadDocumentsAsync(indexDocuments);
    }
}