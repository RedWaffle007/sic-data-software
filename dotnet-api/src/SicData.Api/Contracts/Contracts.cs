using System.ComponentModel.DataAnnotations;

namespace SicData.Api.Contracts;

// ── Requests ─────────────────────────────────────────────────────────────────

public record CreateDatasetRequest
{
    [Required, MaxLength(255)] public string Name { get; init; } = "";
    public string? Description { get; init; }
    [Required] public List<string> SicCodes { get; init; } = new();
    public List<string>? Counties { get; init; }
    public string? SourceFile { get; init; }
    public List<CompanyInput> Companies { get; init; } = new();
}

public record CompanyInput
{
    [Required, MaxLength(8)] public string CompanyNumber { get; init; } = "";
    [Required] public string BusinessName { get; init; } = "";
    public string? AddressLine1 { get; init; }
    public string? AddressLine2 { get; init; }
    public string? Town { get; init; }
    public string? County { get; init; }
    public string? Postcode { get; init; }
    public string? PersonWithSignificantControl { get; init; }
    public string? NatureOfControl { get; init; }
    public string? Title { get; init; }
    public string? Fname { get; init; }
    public string? Sname { get; init; }
    public string? Position { get; init; }
    public string? Sic { get; init; }
    public string? CompanyStatus { get; init; }
    public string? CompanyType { get; init; }
    public string? DateOfCreation { get; init; }
    public string? Website { get; init; }
    public string? Phone { get; init; }
    public string? Email { get; init; }
    public string? WebsiteAddress { get; init; }
    public string? AddressMatch { get; init; }
}

public record UpdateDatasetRequest
{
    [MaxLength(255)] public string? Name { get; init; }
    public string? Description { get; init; }
}

/// <summary>Partial company update (Excel-style cell editing).</summary>
public record UpdateCompanyRequest
{
    public string? BusinessName { get; init; }
    public string? AddressLine1 { get; init; }
    public string? AddressLine2 { get; init; }
    public string? Town { get; init; }
    public string? County { get; init; }
    public string? Postcode { get; init; }
    public string? Title { get; init; }
    public string? Fname { get; init; }
    public string? Sname { get; init; }
    public string? Position { get; init; }
    public string? CompanyStatus { get; init; }
    public string? Website { get; init; }
    public string? Phone { get; init; }
    public string? Email { get; init; }
    public string? WebsiteAddress { get; init; }
    public string? AddressMatch { get; init; }
}

// ── Responses ────────────────────────────────────────────────────────────────

public record DatasetSummary
{
    public int Id { get; init; }
    public string Name { get; init; } = "";
    public string? Description { get; init; }
    public int TotalCompanies { get; init; }
    public List<string> SicCodes { get; init; } = new();
    public List<string>? Counties { get; init; }
    public string? CreatedAt { get; init; }
    public string? UpdatedAt { get; init; }
}

public record CompanyDto
{
    public int Id { get; init; }
    public string CompanyNumber { get; init; } = "";
    public string BusinessName { get; init; } = "";
    public string? AddressLine1 { get; init; }
    public string? AddressLine2 { get; init; }
    public string? Town { get; init; }
    public string? County { get; init; }
    public string? Postcode { get; init; }
    public string? PersonWithSignificantControl { get; init; }
    public string? NatureOfControl { get; init; }
    public string? Title { get; init; }
    public string? Fname { get; init; }
    public string? Sname { get; init; }
    public string? Position { get; init; }
    public string? Sic { get; init; }
    public string? CompanyStatus { get; init; }
    public string? CompanyType { get; init; }
    public string? DateOfCreation { get; init; }
    public string? Website { get; init; }
    public string? Phone { get; init; }
    public string? Email { get; init; }
    public string? WebsiteAddress { get; init; }
    public string? AddressMatch { get; init; }
}

public record PagedCompanies
{
    public int DatasetId { get; init; }
    public string DatasetName { get; init; } = "";
    public int Total { get; init; }
    public int Returned { get; init; }
    public int Skip { get; init; }
    public int Limit { get; init; }
    public List<CompanyDto> Companies { get; init; } = new();
}

public record AnalysisDto
{
    public int TotalCompanies { get; init; }
    public int UniqueCounties { get; init; }
    public double DataQualityScore { get; init; }
    public Dictionary<string, int> RegionalDistribution { get; init; } = new();
    public Dictionary<string, int> MissingData { get; init; } = new();
    public string? GeneratedAt { get; init; }
}

public record SearchDatasetGroup
{
    public int DatasetId { get; init; }
    public string DatasetName { get; init; } = "";
    public int MatchCount { get; init; }
    public List<CompanyDto> Companies { get; init; } = new();
}

public record SearchResponse
{
    public int TotalMatching { get; init; }
    public int Returned { get; init; }
    public List<SearchDatasetGroup> Datasets { get; init; } = new();
    public string Query { get; init; } = "";
    public List<string> FieldsSearched { get; init; } = new();
    public int DatasetsWithMatches { get; init; }
}
