using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using SicData.Api.Domain;

namespace SicData.Api.Data;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
    {
    }

    public DbSet<Dataset> Datasets => Set<Dataset>();
    public DbSet<Company> Companies => Set<Company>();
    public DbSet<DatasetAnalysis> Analyses => Set<DatasetAnalysis>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var listConverter = new ValueConverter<List<string>, string>(
            v => JsonSerializer.Serialize(v, (JsonSerializerOptions?)null),
            v => JsonSerializer.Deserialize<List<string>>(v, (JsonSerializerOptions?)null) ?? new());
        var listComparer = new ValueComparer<List<string>>(
            (a, b) => (a ?? new()).SequenceEqual(b ?? new()),
            v => v == null ? 0 : v.Aggregate(0, (h, s) => HashCode.Combine(h, s.GetHashCode())),
            v => v.ToList());

        var nullableListConverter = new ValueConverter<List<string>?, string?>(
            v => v == null ? null : JsonSerializer.Serialize(v, (JsonSerializerOptions?)null),
            v => v == null ? null : JsonSerializer.Deserialize<List<string>>(v, (JsonSerializerOptions?)null));

        var intDictConverter = new ValueConverter<Dictionary<string, int>, string>(
            v => JsonSerializer.Serialize(v, (JsonSerializerOptions?)null),
            v => JsonSerializer.Deserialize<Dictionary<string, int>>(v, (JsonSerializerOptions?)null) ?? new());
        var strDictConverter = new ValueConverter<Dictionary<string, string>, string>(
            v => JsonSerializer.Serialize(v, (JsonSerializerOptions?)null),
            v => JsonSerializer.Deserialize<Dictionary<string, string>>(v, (JsonSerializerOptions?)null) ?? new());

        modelBuilder.Entity<Dataset>(e =>
        {
            e.ToTable("datasets");
            e.HasKey(d => d.Id);
            e.Property(d => d.Id).HasColumnName("id");
            e.Property(d => d.Name).HasColumnName("name").HasMaxLength(255).IsRequired();
            e.HasIndex(d => d.Name).IsUnique();
            e.Property(d => d.Description).HasColumnName("description");
            e.Property(d => d.SicCodes).HasColumnName("sic_codes").HasConversion(listConverter, listComparer).IsRequired();
            e.Property(d => d.Counties).HasColumnName("counties").HasConversion(nullableListConverter);
            e.Property(d => d.TotalCompanies).HasColumnName("total_companies");
            e.Property(d => d.SourceFile).HasColumnName("source_file").HasMaxLength(500);
            e.Property(d => d.CreatedAt).HasColumnName("created_at");
            e.Property(d => d.UpdatedAt).HasColumnName("updated_at");
            e.HasMany(d => d.Companies).WithOne(c => c.Dataset).HasForeignKey(c => c.DatasetId).OnDelete(DeleteBehavior.Cascade);
            e.HasOne(d => d.Analysis).WithOne(a => a.Dataset).HasForeignKey<DatasetAnalysis>(a => a.DatasetId).OnDelete(DeleteBehavior.Cascade);
        });

        modelBuilder.Entity<Company>(e =>
        {
            e.ToTable("companies");
            e.HasKey(c => c.Id);
            e.Property(c => c.Id).HasColumnName("id");
            e.Property(c => c.DatasetId).HasColumnName("dataset_id");
            e.Property(c => c.CompanyNumber).HasColumnName("company_number").HasMaxLength(8).IsRequired();
            e.Property(c => c.BusinessName).HasColumnName("business_name").IsRequired();
            e.Property(c => c.AddressLine1).HasColumnName("address_line1");
            e.Property(c => c.AddressLine2).HasColumnName("address_line2");
            e.Property(c => c.Town).HasColumnName("town");
            e.Property(c => c.County).HasColumnName("county").HasMaxLength(100);
            e.Property(c => c.Postcode).HasColumnName("postcode").HasMaxLength(10);
            e.Property(c => c.PersonWithSignificantControl).HasColumnName("person_with_significant_control");
            e.Property(c => c.NatureOfControl).HasColumnName("nature_of_control");
            e.Property(c => c.Title).HasColumnName("title").HasMaxLength(50);
            e.Property(c => c.Fname).HasColumnName("fname").HasMaxLength(100);
            e.Property(c => c.Sname).HasColumnName("sname").HasMaxLength(100);
            e.Property(c => c.SelectedPersonSource).HasColumnName("selected_person_source");
            e.Property(c => c.SelectedPscShareTier).HasColumnName("selected_psc_share_tier").HasMaxLength(20);
            e.Property(c => c.SelectedPscNatureOfControl).HasColumnName("selected_psc_nature_of_control");
            e.Property(c => c.Position).HasColumnName("position");
            e.Property(c => c.Sic).HasColumnName("sic");
            e.Property(c => c.CompanyStatus).HasColumnName("company_status").HasMaxLength(50);
            e.Property(c => c.CompanyType).HasColumnName("company_type").HasMaxLength(100);
            e.Property(c => c.DateOfCreation).HasColumnName("date_of_creation").HasMaxLength(20);
            e.Property(c => c.Website).HasColumnName("website");
            e.Property(c => c.Phone).HasColumnName("phone").HasMaxLength(50);
            e.Property(c => c.Email).HasColumnName("email").HasMaxLength(255);
            e.Property(c => c.WebsiteAddress).HasColumnName("website_address");
            e.Property(c => c.AddressMatch).HasColumnName("address_match").HasMaxLength(50);
            e.Property(c => c.CreatedAt).HasColumnName("created_at");
            e.Property(c => c.UpdatedAt).HasColumnName("updated_at");
            e.HasIndex(c => c.CompanyNumber);
            e.HasIndex(c => c.County);
            e.HasIndex(c => c.Postcode);
            e.HasIndex(c => new { c.DatasetId, c.County });
        });

        modelBuilder.Entity<DatasetAnalysis>(e =>
        {
            e.ToTable("dataset_analysis");
            e.HasKey(a => a.Id);
            e.Property(a => a.Id).HasColumnName("id");
            e.Property(a => a.DatasetId).HasColumnName("dataset_id");
            e.HasIndex(a => a.DatasetId).IsUnique();
            e.Property(a => a.TotalCompanies).HasColumnName("total_companies");
            e.Property(a => a.UniqueCounties).HasColumnName("unique_counties");
            e.Property(a => a.DataQualityScore).HasColumnName("data_quality_score");
            e.Property(a => a.RegionalDistribution).HasColumnName("regional_distribution").HasConversion(intDictConverter);
            e.Property(a => a.CountyResolution).HasColumnName("county_resolution").HasConversion(strDictConverter);
            e.Property(a => a.MissingData).HasColumnName("missing_data").HasConversion(intDictConverter);
            e.Property(a => a.GeneratedAt).HasColumnName("generated_at");
        });
    }
}
