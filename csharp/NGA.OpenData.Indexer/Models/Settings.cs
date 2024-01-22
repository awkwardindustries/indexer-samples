namespace NGA.OpenData.Indexer.Models;

public class Settings
{
    public string AzureAiVisionKey { get; set; }
    public string AzureAiVisionEndpoint { get; set; }
    public string AzureAiSearchKey { get; set; }
    public string AzureAiSearchEndpoint { get; set; }
    public string AzureAiSearchIndexName { get; set; }
    public bool DropAndRecreateIndexIfItExists { get; set; }
    public string NgaOpenDataPostgresqlConnectionString { get; set; }

    public Settings()
    {
        this.AzureAiVisionKey = string.Empty;
        this.AzureAiVisionEndpoint = string.Empty;
        this.AzureAiSearchKey = string.Empty;
        this.AzureAiSearchEndpoint = string.Empty;
        this.AzureAiSearchIndexName = string.Empty;
        this.DropAndRecreateIndexIfItExists = true;
        this.NgaOpenDataPostgresqlConnectionString = string.Empty;
    }
}