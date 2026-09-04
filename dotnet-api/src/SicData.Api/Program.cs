using Microsoft.EntityFrameworkCore;
using SicData.Api.Contracts;
using SicData.Api.Data;
using SicData.Api.Services;

var builder = WebApplication.CreateBuilder(args);

// ── Logging ──────────────────────────────────────────────────────────────────
builder.Logging.ClearProviders();
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
});

// ── Database (SQLite by default, PostgreSQL opt-in to share the Python DB) ───
var dbProvider = builder.Configuration.GetValue<string>("Database:Provider") ?? "Sqlite";
var connectionString = builder.Configuration.GetConnectionString("Default") ?? "Data Source=sicdata.db";
builder.Services.AddDbContext<AppDbContext>(options =>
{
    if (dbProvider.Equals("Postgres", StringComparison.OrdinalIgnoreCase)
        || dbProvider.Equals("PostgreSQL", StringComparison.OrdinalIgnoreCase))
        options.UseNpgsql(connectionString);
    else
        options.UseSqlite(connectionString);
});

builder.Services.AddScoped<SicDataService>();

const string CorsPolicy = "frontend";
builder.Services.AddCors(o => o.AddPolicy(CorsPolicy, p =>
{
    var origins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>()
        ?? new[] { "http://localhost:5173", "http://localhost:8000" };
    p.WithOrigins(origins).AllowAnyHeader().AllowAnyMethod();
}));

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(o => o.SwaggerDoc("v1", new()
{
    Title = "SIC Data Software — Dataset CRUD & Search API (.NET)",
    Version = "v1",
    Description = "ASP.NET Core + EF Core service for dataset/company CRUD, search, analysis and export. "
                + "Python remains the batch/AI engine; .NET owns transactional CRUD and querying.",
}));
builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<DomainExceptionHandler>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    if (db.Database.IsSqlite())
        db.Database.Migrate();
    else
        db.Database.EnsureCreated();
}

app.UseExceptionHandler();
app.UseStatusCodePages();
app.UseSwagger();
app.UseSwaggerUI(o => o.SwaggerEndpoint("/swagger/v1/swagger.json", "SIC Data API v1"));
app.UseCors(CorsPolicy);

// ── Dataset endpoints ────────────────────────────────────────────────────────

app.MapGet("/api/datasets/health", () => Results.Ok(new { status = "healthy", service = "dataset-management" }));

app.MapGet("/api/datasets", async (SicDataService svc, int? skip, int? limit, CancellationToken ct) =>
{
    var datasets = await svc.ListDatasetsAsync(Math.Max(0, skip ?? 0), Math.Clamp(limit ?? 100, 1, 1000), ct);
    return Results.Ok(new { total = datasets.Count, datasets });
})
    .WithSummary("List datasets (paginated).");

app.MapPost("/api/datasets", async (CreateDatasetRequest req, SicDataService svc, CancellationToken ct) =>
{
    var created = await svc.CreateDatasetAsync(req, ct);
    return Results.Created($"/api/datasets/{created.Id}", created);
})
    .WithSummary("Create a dataset with its companies (JSON). Parquet import stays in the Python service.");

app.MapGet("/api/datasets/{id:int}", async (int id, SicDataService svc, CancellationToken ct) =>
    Results.Ok(await svc.GetDatasetAsync(id, ct)))
    .WithSummary("Get a dataset.");

app.MapPut("/api/datasets/{id:int}", async (int id, UpdateDatasetRequest req, SicDataService svc, CancellationToken ct) =>
    Results.Ok(await svc.UpdateDatasetAsync(id, req, ct)))
    .WithSummary("Update dataset metadata.");

app.MapDelete("/api/datasets/{id:int}", async (int id, SicDataService svc, CancellationToken ct) =>
{
    await svc.DeleteDatasetAsync(id, ct);
    return Results.Ok(new { success = true, message = "Dataset deleted successfully" });
})
    .WithSummary("Delete a dataset and its companies.");

// ── Company endpoints ──────────────────────────────────────────────────────

app.MapGet("/api/datasets/{id:int}/companies", async (int id, SicDataService svc, int? skip, int? limit, string? county, CancellationToken ct) =>
    Results.Ok(await svc.GetCompaniesAsync(id, Math.Max(0, skip ?? 0), Math.Clamp(limit ?? 10000, 1, 50000), county, ct)))
    .WithSummary("List companies in a dataset (paginated, optional county filter).");

app.MapPut("/api/datasets/{id:int}/companies/{companyId:int}", async (int id, int companyId, UpdateCompanyRequest req, SicDataService svc, CancellationToken ct) =>
    Results.Ok(await svc.UpdateCompanyAsync(companyId, req, id, ct)))
    .WithSummary("Update a company within a dataset.");

app.MapPatch("/api/companies/{companyId:int}", async (int companyId, UpdateCompanyRequest req, SicDataService svc, CancellationToken ct) =>
    Results.Ok(await svc.UpdateCompanyAsync(companyId, req, null, ct)))
    .WithSummary("Partial company update (Excel-style cell editing).");

app.MapDelete("/api/datasets/{id:int}/companies/{companyId:int}", async (int id, int companyId, SicDataService svc, CancellationToken ct) =>
{
    await svc.DeleteCompanyAsync(companyId, ct);
    return Results.Ok(new { success = true, message = "Company deleted successfully" });
})
    .WithSummary("Delete a company.");

// ── Analysis endpoints ─────────────────────────────────────────────────────

app.MapPost("/api/datasets/{id:int}/analyze", async (int id, SicDataService svc, CancellationToken ct) =>
    Results.Ok(await svc.RegenerateAnalysisAsync(id, ct)))
    .WithSummary("Regenerate dataset analysis (quality, regional distribution, missing data).");

app.MapGet("/api/datasets/{id:int}/analysis", async (int id, SicDataService svc, CancellationToken ct) =>
    Results.Ok(await svc.GetAnalysisAsync(id, ct)))
    .WithSummary("Get cached analysis.");

// ── Search + export ────────────────────────────────────────────────────────

app.MapGet("/api/search", async (SicDataService svc, string q, int? skip, int? limit, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(q))
        return Results.Problem("Query 'q' is required.", statusCode: 400);
    return Results.Ok(await svc.SearchAsync(q, Math.Max(0, skip ?? 0), Math.Clamp(limit ?? 500, 1, 2000), ct));
})
    .WithSummary("Global search across all datasets and many company fields.");

app.MapGet("/api/datasets/{id:int}/export", async (int id, SicDataService svc, CancellationToken ct) =>
{
    var (content, fileName) = await svc.ExportCsvAsync(id, ct);
    return Results.File(content, "text/csv", fileName);
})
    .WithSummary("Export a dataset to CSV.");

app.Run();

public partial class Program { }
