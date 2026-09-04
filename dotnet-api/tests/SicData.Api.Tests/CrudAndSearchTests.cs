using System.Net;
using System.Net.Http.Json;
using SicData.Api.Contracts;
using Xunit;

namespace SicData.Api.Tests;

public class CrudAndSearchTests : IClassFixture<SicWebAppFactory>
{
    private readonly HttpClient _client;

    public CrudAndSearchTests(SicWebAppFactory factory) => _client = factory.CreateClient();

    private static CreateDatasetRequest SampleDataset(string name) => new()
    {
        Name = name,
        Description = "test dataset",
        SicCodes = new() { "62012", "62020" },
        Counties = new() { "Essex", "Kent" },
        Companies = new()
        {
            new CompanyInput { CompanyNumber = "00000001", BusinessName = "Acme Software Ltd", County = "Essex", Postcode = "CM1 1AA", Email = "info@acme.test", Sic = "62012" },
            new CompanyInput { CompanyNumber = "00000002", BusinessName = "Beta Cloud Ltd", County = "Kent", Postcode = "ME1 1BB", Sic = "62020" },
            new CompanyInput { CompanyNumber = "00000003", BusinessName = "Gamma Systems", County = "Essex", Sic = "62012" },
        },
    };

    [Fact]
    public async Task Create_get_update_delete_dataset_lifecycle()
    {
        var create = await _client.PostAsJsonAsync("/api/datasets", SampleDataset("Lifecycle DS"));
        Assert.Equal(HttpStatusCode.Created, create.StatusCode);
        var ds = await create.Content.ReadFromJsonAsync<DatasetSummary>();
        Assert.Equal(3, ds!.TotalCompanies);
        Assert.Equal(new[] { "62012", "62020" }, ds.SicCodes);

        var got = await _client.GetFromJsonAsync<DatasetSummary>($"/api/datasets/{ds.Id}");
        Assert.Equal("Lifecycle DS", got!.Name);

        var upd = await _client.PutAsJsonAsync($"/api/datasets/{ds.Id}", new UpdateDatasetRequest { Description = "updated" });
        upd.EnsureSuccessStatusCode();
        Assert.Equal("updated", (await upd.Content.ReadFromJsonAsync<DatasetSummary>())!.Description);

        var del = await _client.DeleteAsync($"/api/datasets/{ds.Id}");
        del.EnsureSuccessStatusCode();
        Assert.Equal(HttpStatusCode.NotFound, (await _client.GetAsync($"/api/datasets/{ds.Id}")).StatusCode);
    }

    [Fact]
    public async Task Duplicate_dataset_name_conflicts()
    {
        (await _client.PostAsJsonAsync("/api/datasets", SampleDataset("Dupe DS"))).EnsureSuccessStatusCode();
        var second = await _client.PostAsJsonAsync("/api/datasets", SampleDataset("Dupe DS"));
        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
    }

    [Fact]
    public async Task Companies_are_paginated_and_county_filterable()
    {
        var ds = await (await _client.PostAsJsonAsync("/api/datasets", SampleDataset("Companies DS"))).Content.ReadFromJsonAsync<DatasetSummary>();

        var all = await _client.GetFromJsonAsync<PagedCompanies>($"/api/datasets/{ds!.Id}/companies");
        Assert.Equal(3, all!.Total);

        var essex = await _client.GetFromJsonAsync<PagedCompanies>($"/api/datasets/{ds.Id}/companies?county=Essex");
        Assert.Equal(2, essex!.Total);

        var firstPage = await _client.GetFromJsonAsync<PagedCompanies>($"/api/datasets/{ds.Id}/companies?skip=0&limit=1");
        Assert.Equal(1, firstPage!.Returned);
        Assert.Equal(3, firstPage.Total);
    }

    [Fact]
    public async Task Patch_company_updates_single_field()
    {
        var ds = await (await _client.PostAsJsonAsync("/api/datasets", SampleDataset("Patch DS"))).Content.ReadFromJsonAsync<DatasetSummary>();
        var companies = await _client.GetFromJsonAsync<PagedCompanies>($"/api/datasets/{ds!.Id}/companies");
        var target = companies!.Companies.First(c => c.CompanyNumber == "00000003");

        var patch = await _client.PatchAsJsonAsync($"/api/companies/{target.Id}", new UpdateCompanyRequest { County = "Suffolk" });
        patch.EnsureSuccessStatusCode();
        var updated = await patch.Content.ReadFromJsonAsync<CompanyDto>();
        Assert.Equal("Suffolk", updated!.County);
        Assert.Equal("Gamma Systems", updated.BusinessName); // untouched
    }

    [Fact]
    public async Task Analysis_computes_quality_and_regional_distribution()
    {
        var ds = await (await _client.PostAsJsonAsync("/api/datasets", SampleDataset("Analysis DS"))).Content.ReadFromJsonAsync<DatasetSummary>();

        var analysis = await (await _client.PostAsync($"/api/datasets/{ds!.Id}/analyze", null)).Content.ReadFromJsonAsync<AnalysisDto>();
        Assert.Equal(3, analysis!.TotalCompanies);
        Assert.Equal(2, analysis.UniqueCounties);
        Assert.Equal(2, analysis.RegionalDistribution["Essex"]);
        Assert.True(analysis.DataQualityScore is > 0 and <= 100);
        // Two companies have no email → missing email count is 2.
        Assert.Equal(2, analysis.MissingData["email"]);

        var cached = await _client.GetFromJsonAsync<AnalysisDto>($"/api/datasets/{ds.Id}/analysis");
        Assert.Equal(analysis.TotalCompanies, cached!.TotalCompanies);
    }

    [Fact]
    public async Task Search_finds_across_datasets_and_fields()
    {
        await _client.PostAsJsonAsync("/api/datasets", SampleDataset("Search DS A"));
        await _client.PostAsJsonAsync("/api/datasets", SampleDataset("Search DS B") with { Name = "Search DS B" });

        var results = await _client.GetFromJsonAsync<SearchResponse>("/api/search?q=Software");
        Assert.True(results!.TotalMatching >= 2); // "Acme Software Ltd" in both datasets
        Assert.All(results.Datasets, g => Assert.All(g.Companies, c =>
            Assert.Contains("Software", c.BusinessName)));

        var byPostcode = await _client.GetFromJsonAsync<SearchResponse>("/api/search?q=CM1");
        Assert.True(byPostcode!.TotalMatching >= 2);
    }

    [Fact]
    public async Task Export_returns_csv_with_headers_and_rows()
    {
        var ds = await (await _client.PostAsJsonAsync("/api/datasets", SampleDataset("Export DS"))).Content.ReadFromJsonAsync<DatasetSummary>();
        var resp = await _client.GetAsync($"/api/datasets/{ds!.Id}/export");
        resp.EnsureSuccessStatusCode();
        Assert.Equal("text/csv", resp.Content.Headers.ContentType!.MediaType);
        var csv = await resp.Content.ReadAsStringAsync();
        var lines = csv.Trim().Split('\n');
        Assert.StartsWith("CompanyNumber,BusinessName", lines[0]);
        Assert.Equal(4, lines.Length); // header + 3 companies
        Assert.Contains("Acme Software Ltd", csv);
    }

    [Fact]
    public async Task Missing_dataset_returns_404()
    {
        Assert.Equal(HttpStatusCode.NotFound, (await _client.GetAsync("/api/datasets/999999")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await _client.GetAsync("/api/datasets/999999/analysis")).StatusCode);
    }

    [Fact]
    public async Task Swagger_is_served()
    {
        var resp = await _client.GetAsync("/swagger/v1/swagger.json");
        resp.EnsureSuccessStatusCode();
        Assert.Contains("/api/datasets", await resp.Content.ReadAsStringAsync());
    }
}
