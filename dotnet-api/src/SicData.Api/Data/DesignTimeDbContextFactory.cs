using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace SicData.Api.Data;

/// <summary>Used by <c>dotnet ef</c> so migration commands don't boot the web host.</summary>
public class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<AppDbContext>
{
    public AppDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseSqlite("Data Source=sicdata.db")
            .Options;
        return new AppDbContext(options);
    }
}
