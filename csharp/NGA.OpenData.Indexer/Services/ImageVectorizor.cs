using System.Net.Http.Json;
using System.Text;
using NGA.OpenData.Indexer.Models;

namespace NGA.OpenData.Indexer.Services;

public class ImageVectorizer(IHttpClientFactory httpClientFactory, string aiVisionKey, string aiVisionEndpoint)
{
    private readonly IHttpClientFactory _httpClientFactory = httpClientFactory;
    private readonly string _aiVisionKey = aiVisionKey;
    private readonly string _aiVisionEndpoint = aiVisionEndpoint;

    public async Task<IEnumerable<float>> VectorizeImage(string imageUrl)
    {
        var httpClient = _httpClientFactory.CreateClient(Constants.IMAGE_VECTORIZOR_HTTP_CLIENT);
        var requestContent = new StringContent(
            $"{{ 'url': '{imageUrl}' }}",
            Encoding.UTF8,
            "application/json"
        );
        httpClient.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", _aiVisionKey);

        using HttpResponseMessage response = await httpClient.PostAsync(
            $"{_aiVisionEndpoint}computervision/retrieval:vectorizeImage?api-version=2023-02-01-preview&modelVersion=latest",
            requestContent);

        response.EnsureSuccessStatusCode();        

        var vectorizeImageResponse = await response.Content.ReadFromJsonAsync<VectorizeImageResponse>() 
            ?? throw new Exception("Response not received or could not be serialized as expected.");

        return vectorizeImageResponse.Vector;
    }
}