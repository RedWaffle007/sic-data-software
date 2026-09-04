using SicData.Api.Contracts;
using SicData.Api.Domain;

namespace SicData.Api.Services;

public static class Mappers
{
    public static DatasetSummary ToSummary(this Dataset d) => new()
    {
        Id = d.Id,
        Name = d.Name,
        Description = d.Description,
        TotalCompanies = d.TotalCompanies,
        SicCodes = d.SicCodes,
        Counties = d.Counties,
        CreatedAt = d.CreatedAt?.ToString("O"),
        UpdatedAt = d.UpdatedAt?.ToString("O"),
    };

    public static CompanyDto ToDto(this Company c) => new()
    {
        Id = c.Id,
        CompanyNumber = c.CompanyNumber,
        BusinessName = c.BusinessName,
        AddressLine1 = c.AddressLine1,
        AddressLine2 = c.AddressLine2,
        Town = c.Town,
        County = c.County,
        Postcode = c.Postcode,
        PersonWithSignificantControl = c.PersonWithSignificantControl,
        NatureOfControl = c.NatureOfControl,
        Title = c.Title,
        Fname = c.Fname,
        Sname = c.Sname,
        Position = c.Position,
        Sic = c.Sic,
        CompanyStatus = c.CompanyStatus,
        CompanyType = c.CompanyType,
        DateOfCreation = c.DateOfCreation,
        Website = c.Website,
        Phone = c.Phone,
        Email = c.Email,
        WebsiteAddress = c.WebsiteAddress,
        AddressMatch = c.AddressMatch,
    };

    public static Company ToEntity(this CompanyInput i) => new()
    {
        CompanyNumber = i.CompanyNumber,
        BusinessName = i.BusinessName,
        AddressLine1 = i.AddressLine1,
        AddressLine2 = i.AddressLine2,
        Town = i.Town,
        County = i.County,
        Postcode = i.Postcode,
        PersonWithSignificantControl = i.PersonWithSignificantControl,
        NatureOfControl = i.NatureOfControl,
        Title = i.Title,
        Fname = i.Fname,
        Sname = i.Sname,
        Position = i.Position,
        Sic = i.Sic,
        CompanyStatus = i.CompanyStatus,
        CompanyType = i.CompanyType,
        DateOfCreation = i.DateOfCreation,
        Website = i.Website,
        Phone = i.Phone,
        Email = i.Email,
        WebsiteAddress = i.WebsiteAddress,
        AddressMatch = i.AddressMatch,
        CreatedAt = DateTime.UtcNow,
        UpdatedAt = DateTime.UtcNow,
    };
}
