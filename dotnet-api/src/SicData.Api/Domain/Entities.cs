namespace SicData.Api.Domain;

/// <summary>
/// A "sheet" — a collection of companies extracted with specific criteria.
/// Table/column names mirror the SQLAlchemy schema so this service can share the
/// same PostgreSQL database as the Python ETL/enrichment engine.
/// </summary>
public class Dataset
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public string? Description { get; set; }

    /// <summary>List of SIC codes (stored as JSON).</summary>
    public List<string> SicCodes { get; set; } = new();

    /// <summary>Optional county filters (stored as JSON).</summary>
    public List<string>? Counties { get; set; }

    public int TotalCompanies { get; set; }
    public string? SourceFile { get; set; }

    public DateTime? CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }

    public List<Company> Companies { get; set; } = new();
    public DatasetAnalysis? Analysis { get; set; }
}

/// <summary>Individual company record within a dataset.</summary>
public class Company
{
    public int Id { get; set; }
    public int DatasetId { get; set; }

    public string CompanyNumber { get; set; } = "";
    public string BusinessName { get; set; } = "";

    public string? AddressLine1 { get; set; }
    public string? AddressLine2 { get; set; }
    public string? Town { get; set; }
    public string? County { get; set; }
    public string? Postcode { get; set; }

    public string? PersonWithSignificantControl { get; set; }
    public string? NatureOfControl { get; set; }
    public string? Title { get; set; }
    public string? Fname { get; set; }
    public string? Sname { get; set; }

    public string? SelectedPersonSource { get; set; }
    public string? SelectedPscShareTier { get; set; }
    public string? SelectedPscNatureOfControl { get; set; }

    public string? Position { get; set; }

    public string? Sic { get; set; }
    public string? CompanyStatus { get; set; }
    public string? CompanyType { get; set; }
    public string? DateOfCreation { get; set; }

    public string? Website { get; set; }
    public string? Phone { get; set; }
    public string? Email { get; set; }
    public string? WebsiteAddress { get; set; }
    public string? AddressMatch { get; set; }

    public DateTime? CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }

    public Dataset? Dataset { get; set; }
}

/// <summary>Cached analysis results for a dataset (regenerated on edit).</summary>
public class DatasetAnalysis
{
    public int Id { get; set; }
    public int DatasetId { get; set; }

    public int TotalCompanies { get; set; }
    public int UniqueCounties { get; set; }
    public double DataQualityScore { get; set; }

    public Dictionary<string, int> RegionalDistribution { get; set; } = new();
    public Dictionary<string, string> CountyResolution { get; set; } = new();
    public Dictionary<string, int> MissingData { get; set; } = new();

    public DateTime? GeneratedAt { get; set; }

    public Dataset? Dataset { get; set; }
}
