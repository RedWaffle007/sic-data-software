namespace SicData.Api.Services;

/// <summary>Raised for domain/validation failures; mapped to 4xx by the endpoints.</summary>
public class DomainException : Exception
{
    public int StatusCode { get; }
    public DomainException(string message, int statusCode = 400) : base(message) => StatusCode = statusCode;
}
