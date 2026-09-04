using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http;

namespace SicData.Api.Services;

/// <summary>Maps <see cref="DomainException"/> to the right 4xx + ProblemDetails.</summary>
public class DomainExceptionHandler : IExceptionHandler
{
    private readonly ILogger<DomainExceptionHandler> _log;

    public DomainExceptionHandler(ILogger<DomainExceptionHandler> log) => _log = log;

    public async ValueTask<bool> TryHandleAsync(HttpContext http, Exception exception, CancellationToken ct)
    {
        if (exception is not DomainException domain)
            return false;

        _log.LogWarning("Domain error {Status}: {Message}", domain.StatusCode, domain.Message);
        http.Response.StatusCode = domain.StatusCode;
        await http.Response.WriteAsJsonAsync(new
        {
            type = "about:blank",
            title = domain.Message,
            status = domain.StatusCode,
        }, ct);
        return true;
    }
}
