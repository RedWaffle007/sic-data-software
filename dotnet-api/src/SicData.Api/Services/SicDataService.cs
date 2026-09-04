using System.Text;
using Microsoft.EntityFrameworkCore;
using SicData.Api.Contracts;
using SicData.Api.Data;
using SicData.Api.Domain;

namespace SicData.Api.Services;

/// <summary>
/// Transactional CRUD + search + analysis over datasets and companies. The Python
/// side stays the batch/AI engine (spreadsheet parsing, enrichment, letters);
/// this service owns conventional relational CRUD, querying and exports.
/// </summary>
public class SicDataService
{
    private readonly AppDbContext _db;
    private readonly ILogger<SicDataService> _log;

    public SicDataService(AppDbContext db, ILogger<SicDataService> log)
    {
        _db = db;
        _log = log;
    }

    // ── Datasets ─────────────────────────────────────────────────────────────

    public async Task<IReadOnlyList<DatasetSummary>> ListDatasetsAsync(int skip, int limit, CancellationToken ct)
    {
        var rows = await _db.Datasets.AsNoTracking()
            .OrderByDescending(d => d.Id)
            .Skip(skip).Take(limit)
            .ToListAsync(ct);
        return rows.Select(d => d.ToSummary()).ToList();
    }

    public async Task<DatasetSummary> GetDatasetAsync(int id, CancellationToken ct)
    {
        var d = await _db.Datasets.AsNoTracking().FirstOrDefaultAsync(x => x.Id == id, ct)
            ?? throw new DomainException("Dataset not found", 404);
        return d.ToSummary();
    }

    public async Task<DatasetSummary> CreateDatasetAsync(CreateDatasetRequest req, CancellationToken ct)
    {
        if (await _db.Datasets.AnyAsync(d => d.Name == req.Name, ct))
            throw new DomainException($"A dataset named '{req.Name}' already exists.", 409);

        var dataset = new Dataset
        {
            Name = req.Name,
            Description = req.Description,
            SicCodes = req.SicCodes,
            Counties = req.Counties,
            SourceFile = req.SourceFile,
            TotalCompanies = req.Companies.Count,
            CreatedAt = DateTime.UtcNow,
            UpdatedAt = DateTime.UtcNow,
            Companies = req.Companies.Select(c => c.ToEntity()).ToList(),
        };
        _db.Datasets.Add(dataset);
        await _db.SaveChangesAsync(ct);
        _log.LogInformation("Created dataset {Name} ({Count} companies)", dataset.Name, dataset.TotalCompanies);
        return dataset.ToSummary();
    }

    public async Task<DatasetSummary> UpdateDatasetAsync(int id, UpdateDatasetRequest req, CancellationToken ct)
    {
        var d = await _db.Datasets.FirstOrDefaultAsync(x => x.Id == id, ct)
            ?? throw new DomainException("Dataset not found", 404);

        if (req.Name is not null)
        {
            if (await _db.Datasets.AnyAsync(x => x.Name == req.Name && x.Id != id, ct))
                throw new DomainException($"A dataset named '{req.Name}' already exists.", 409);
            d.Name = req.Name;
        }
        if (req.Description is not null) d.Description = req.Description;
        d.UpdatedAt = DateTime.UtcNow;
        await _db.SaveChangesAsync(ct);
        return d.ToSummary();
    }

    public async Task DeleteDatasetAsync(int id, CancellationToken ct)
    {
        var d = await _db.Datasets.FirstOrDefaultAsync(x => x.Id == id, ct)
            ?? throw new DomainException("Dataset not found", 404);
        _db.Datasets.Remove(d);
        await _db.SaveChangesAsync(ct);
    }

    // ── Companies ────────────────────────────────────────────────────────────

    public async Task<PagedCompanies> GetCompaniesAsync(int datasetId, int skip, int limit, string? county, CancellationToken ct)
    {
        var dataset = await _db.Datasets.AsNoTracking().FirstOrDefaultAsync(d => d.Id == datasetId, ct)
            ?? throw new DomainException("Dataset not found", 404);

        var query = _db.Companies.AsNoTracking().Where(c => c.DatasetId == datasetId);
        if (!string.IsNullOrWhiteSpace(county))
            query = query.Where(c => c.County == county);

        var total = await query.CountAsync(ct);
        var rows = await query.OrderBy(c => c.Id).Skip(skip).Take(limit).ToListAsync(ct);

        return new PagedCompanies
        {
            DatasetId = datasetId,
            DatasetName = dataset.Name,
            Total = total,
            Returned = rows.Count,
            Skip = skip,
            Limit = limit,
            Companies = rows.Select(c => c.ToDto()).ToList(),
        };
    }

    public async Task<CompanyDto> UpdateCompanyAsync(int companyId, UpdateCompanyRequest req, int? datasetId, CancellationToken ct)
    {
        var c = await _db.Companies.FirstOrDefaultAsync(x => x.Id == companyId, ct)
            ?? throw new DomainException("Company not found", 404);
        if (datasetId is not null && c.DatasetId != datasetId)
            throw new DomainException("Company does not belong to this dataset", 400);

        if (req.BusinessName is not null) c.BusinessName = req.BusinessName;
        if (req.AddressLine1 is not null) c.AddressLine1 = req.AddressLine1;
        if (req.AddressLine2 is not null) c.AddressLine2 = req.AddressLine2;
        if (req.Town is not null) c.Town = req.Town;
        if (req.County is not null) c.County = req.County;
        if (req.Postcode is not null) c.Postcode = req.Postcode;
        if (req.Title is not null) c.Title = req.Title;
        if (req.Fname is not null) c.Fname = req.Fname;
        if (req.Sname is not null) c.Sname = req.Sname;
        if (req.Position is not null) c.Position = req.Position;
        if (req.CompanyStatus is not null) c.CompanyStatus = req.CompanyStatus;
        if (req.Website is not null) c.Website = req.Website;
        if (req.Phone is not null) c.Phone = req.Phone;
        if (req.Email is not null) c.Email = req.Email;
        if (req.WebsiteAddress is not null) c.WebsiteAddress = req.WebsiteAddress;
        if (req.AddressMatch is not null) c.AddressMatch = req.AddressMatch;
        c.UpdatedAt = DateTime.UtcNow;

        await _db.SaveChangesAsync(ct);
        return c.ToDto();
    }

    public async Task DeleteCompanyAsync(int companyId, CancellationToken ct)
    {
        var c = await _db.Companies.FirstOrDefaultAsync(x => x.Id == companyId, ct)
            ?? throw new DomainException("Company not found", 404);
        _db.Companies.Remove(c);
        await _db.SaveChangesAsync(ct);
    }

    // ── Analysis ─────────────────────────────────────────────────────────────

    private static readonly (string Field, Func<Company, string?> Get)[] QualityFields =
    {
        ("business_name", c => c.BusinessName),
        ("postcode", c => c.Postcode),
        ("county", c => c.County),
        ("email", c => c.Email),
        ("phone", c => c.Phone),
        ("website", c => c.Website),
        ("person_with_significant_control", c => c.PersonWithSignificantControl),
    };

    public async Task<AnalysisDto> RegenerateAnalysisAsync(int datasetId, CancellationToken ct)
    {
        var dataset = await _db.Datasets.FirstOrDefaultAsync(d => d.Id == datasetId, ct)
            ?? throw new DomainException("Dataset not found", 404);

        var companies = await _db.Companies.AsNoTracking().Where(c => c.DatasetId == datasetId).ToListAsync(ct);

        var regional = companies
            .Where(c => !string.IsNullOrWhiteSpace(c.County))
            .GroupBy(c => c.County!)
            .ToDictionary(g => g.Key, g => g.Count());

        var missing = QualityFields.ToDictionary(
            f => f.Field,
            f => companies.Count(c => string.IsNullOrWhiteSpace(f.Get(c))));

        var totalCells = (double)companies.Count * QualityFields.Length;
        var filledCells = totalCells - missing.Values.Sum();
        var quality = totalCells > 0 ? Math.Round(filledCells / totalCells * 100, 2) : 0.0;

        var existing = await _db.Analyses.FirstOrDefaultAsync(a => a.DatasetId == datasetId, ct);
        if (existing is null)
        {
            existing = new DatasetAnalysis { DatasetId = datasetId };
            _db.Analyses.Add(existing);
        }
        existing.TotalCompanies = companies.Count;
        existing.UniqueCounties = regional.Count;
        existing.DataQualityScore = quality;
        existing.RegionalDistribution = regional;
        existing.MissingData = missing;
        existing.CountyResolution = new();
        existing.GeneratedAt = DateTime.UtcNow;

        dataset.TotalCompanies = companies.Count;
        await _db.SaveChangesAsync(ct);

        return ToAnalysisDto(existing);
    }

    public async Task<AnalysisDto> GetAnalysisAsync(int datasetId, CancellationToken ct)
    {
        var a = await _db.Analyses.AsNoTracking().FirstOrDefaultAsync(x => x.DatasetId == datasetId, ct)
            ?? throw new DomainException("Analysis not found. Run POST /api/datasets/{id}/analyze first.", 404);
        return ToAnalysisDto(a);
    }

    private static AnalysisDto ToAnalysisDto(DatasetAnalysis a) => new()
    {
        TotalCompanies = a.TotalCompanies,
        UniqueCounties = a.UniqueCounties,
        DataQualityScore = a.DataQualityScore,
        RegionalDistribution = a.RegionalDistribution,
        MissingData = a.MissingData,
        GeneratedAt = a.GeneratedAt?.ToString("O"),
    };

    // ── Search ───────────────────────────────────────────────────────────────

    public static readonly string[] SearchFields =
    {
        "business_name", "company_number", "postcode", "town", "county",
        "address_line1", "address_line2", "person_with_significant_control",
        "fname", "sname", "email", "phone", "website", "sic", "company_status",
    };

    public async Task<SearchResponse> SearchAsync(string q, int skip, int limit, CancellationToken ct)
    {
        var like = $"%{q}%";
        // EF.Functions.Like is case-insensitive on SQLite (default) and works on Npgsql via ILIKE mapping when needed.
        var query = _db.Companies.AsNoTracking().Where(c =>
            EF.Functions.Like(c.BusinessName, like) ||
            EF.Functions.Like(c.CompanyNumber, like) ||
            (c.Postcode != null && EF.Functions.Like(c.Postcode, like)) ||
            (c.Town != null && EF.Functions.Like(c.Town, like)) ||
            (c.County != null && EF.Functions.Like(c.County, like)) ||
            (c.AddressLine1 != null && EF.Functions.Like(c.AddressLine1, like)) ||
            (c.AddressLine2 != null && EF.Functions.Like(c.AddressLine2, like)) ||
            (c.PersonWithSignificantControl != null && EF.Functions.Like(c.PersonWithSignificantControl, like)) ||
            (c.Fname != null && EF.Functions.Like(c.Fname, like)) ||
            (c.Sname != null && EF.Functions.Like(c.Sname, like)) ||
            (c.Email != null && EF.Functions.Like(c.Email, like)) ||
            (c.Phone != null && EF.Functions.Like(c.Phone, like)) ||
            (c.Website != null && EF.Functions.Like(c.Website, like)) ||
            (c.Sic != null && EF.Functions.Like(c.Sic, like)) ||
            (c.CompanyStatus != null && EF.Functions.Like(c.CompanyStatus, like)));

        var total = await query.CountAsync(ct);
        var rows = await query.OrderBy(c => c.DatasetId).ThenBy(c => c.Id).Skip(skip).Take(limit).ToListAsync(ct);

        var datasetNames = await _db.Datasets.AsNoTracking()
            .Where(d => rows.Select(r => r.DatasetId).Distinct().Contains(d.Id))
            .ToDictionaryAsync(d => d.Id, d => d.Name, ct);

        var groups = rows
            .GroupBy(c => c.DatasetId)
            .Select(g => new SearchDatasetGroup
            {
                DatasetId = g.Key,
                DatasetName = datasetNames.GetValueOrDefault(g.Key, $"dataset_{g.Key}"),
                MatchCount = g.Count(),
                Companies = g.Select(c => c.ToDto()).ToList(),
            })
            .OrderByDescending(g => g.MatchCount)
            .ToList();

        return new SearchResponse
        {
            TotalMatching = total,
            Returned = rows.Count,
            Datasets = groups,
            Query = q,
            FieldsSearched = SearchFields.ToList(),
            DatasetsWithMatches = groups.Count,
        };
    }

    // ── Export ───────────────────────────────────────────────────────────────

    public async Task<(byte[] Content, string FileName)> ExportCsvAsync(int datasetId, CancellationToken ct)
    {
        var dataset = await _db.Datasets.AsNoTracking().FirstOrDefaultAsync(d => d.Id == datasetId, ct)
            ?? throw new DomainException("Dataset not found", 404);

        var companies = await _db.Companies.AsNoTracking().Where(c => c.DatasetId == datasetId).OrderBy(c => c.Id).ToListAsync(ct);

        var sb = new StringBuilder();
        string[] headers =
        {
            "CompanyNumber", "BusinessName", "AddressLine1", "AddressLine2", "Town", "County", "Postcode",
            "PSC", "NatureOfControl", "Title", "FirstName", "Surname", "Position", "SIC",
            "CompanyStatus", "CompanyType", "DateOfCreation", "Website", "Phone", "Email",
        };
        sb.AppendLine(string.Join(",", headers));

        foreach (var c in companies)
        {
            string[] cells =
            {
                c.CompanyNumber, c.BusinessName, c.AddressLine1 ?? "", c.AddressLine2 ?? "", c.Town ?? "",
                c.County ?? "", c.Postcode ?? "", c.PersonWithSignificantControl ?? "", c.NatureOfControl ?? "",
                c.Title ?? "", c.Fname ?? "", c.Sname ?? "", c.Position ?? "", c.Sic ?? "",
                c.CompanyStatus ?? "", c.CompanyType ?? "", c.DateOfCreation ?? "", c.Website ?? "",
                c.Phone ?? "", c.Email ?? "",
            };
            sb.AppendLine(string.Join(",", cells.Select(Csv)));
        }

        var fileName = $"{dataset.Name.Replace(' ', '_')}.csv";
        return (Encoding.UTF8.GetBytes(sb.ToString()), fileName);
    }

    private static string Csv(string value)
    {
        if (value.Contains(',') || value.Contains('"') || value.Contains('\n'))
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }
}
