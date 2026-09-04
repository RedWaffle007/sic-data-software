# SIC Data Software — Dataset CRUD & Search API (.NET 9)

An **ASP.NET Core 9 + Entity Framework Core** service that owns the conventional
transactional side of the SIC Data platform: **dataset & company CRUD, global
search, per-dataset analysis, and CSV export**. It draws a clean language
boundary — Python stays the batch/AI engine (spreadsheet parsing, Polars/pandas
transformations, enrichment, Word letter generation); .NET owns typed relational
CRUD, querying, migrations and exports.

## Architecture

```
 Vanilla-JS frontend
        │
        ├──────────────▶ Python FastAPI   (upload, SIC extraction, enrichment, letters)
        │                     │  submits processed rows
        │                     ▼
        └──────────────▶ ASP.NET Core API ── EF Core ──▶ SQLite / PostgreSQL
                          datasets · companies · search · analysis · export
```

Table and column names mirror the existing SQLAlchemy schema (`datasets`,
`companies`, `dataset_analysis`, snake_case columns), so the .NET service can
share the same PostgreSQL database the Python side already targets.

## Endpoints

| Method & path | Purpose |
| --- | --- |
| `GET  /api/datasets` | List datasets (paginated) |
| `POST /api/datasets` | Create a dataset with its companies (JSON) |
| `GET  /api/datasets/{id}` | Get a dataset |
| `PUT  /api/datasets/{id}` | Update dataset metadata |
| `DELETE /api/datasets/{id}` | Delete a dataset (cascades to companies) |
| `GET  /api/datasets/{id}/companies` | List companies (paginated, county filter) |
| `PUT  /api/datasets/{id}/companies/{companyId}` | Update a company |
| `PATCH /api/companies/{companyId}` | Partial update (Excel-style cell edit) |
| `DELETE /api/datasets/{id}/companies/{companyId}` | Delete a company |
| `POST /api/datasets/{id}/analyze` | Regenerate analysis |
| `GET  /api/datasets/{id}/analysis` | Get cached analysis |
| `GET  /api/search?q=` | Global search across datasets + many fields |
| `GET  /api/datasets/{id}/export` | Export a dataset to CSV |
| `GET  /api/datasets/health` | Health check |

Interactive docs: **Swagger UI at `/swagger`**.

## Highlights

- **EF Core** with async **LINQ** queries, relational indexes, cascade deletes,
  and JSON-backed columns (SIC codes, counties, analysis breakdowns).
- **EF Core migrations** (`Data/Migrations/`) applied automatically at startup on
  SQLite; regenerate for Npgsql when targeting PostgreSQL.
- **Analysis**: computes data-quality score, unique counties, regional
  distribution and per-field missing-data counts.
- **Search**: case-insensitive `LIKE`/`ILIKE` across 15+ fields, grouped by
  dataset with match counts.
- **Typed request validation**, ProblemDetails error handling, structured
  logging, environment-based configuration, and CORS for the existing frontend.

## Project layout

```
dotnet-api/
├── SicData.sln
├── docker-compose.yml               # PostgreSQL + API
├── src/SicData.Api/
│   ├── Program.cs                   # Minimal API, DI, config, CORS, Swagger
│   ├── Domain/                      # Entities
│   ├── Data/                        # AppDbContext + EF Core migrations
│   ├── Services/                    # SicDataService, mappers, error handling
│   ├── Contracts/                   # Request/response DTOs + validation
│   └── Dockerfile
└── tests/SicData.Api.Tests/         # xUnit integration tests
```

## Prerequisites

- [.NET SDK 9.0](https://dotnet.microsoft.com/download)
- (Optional) `dotnet-ef` for migrations
- (Optional) Docker + Compose for the PostgreSQL path

## Run locally (SQLite)

```bash
cd src/SicData.Api
dotnet run
# API on http://localhost:5100, Swagger at http://localhost:5100/swagger
```

Quick walk-through:

```bash
B=http://localhost:5100
curl -X POST $B/api/datasets -H 'Content-Type: application/json' -d '{
  "name":"Essex Tech","sicCodes":["62012"],"counties":["Essex"],
  "companies":[{"companyNumber":"00000001","businessName":"Acme Software Ltd","county":"Essex","postcode":"CM1 1AA","sic":"62012"}]
}'
curl -X POST $B/api/datasets/1/analyze
curl "$B/api/search?q=Acme"
curl "$B/api/datasets/1/export" -o essex_tech.csv
```

## Run with PostgreSQL (Docker)

```bash
docker compose up --build     # API on http://localhost:5100, Postgres on 5434
```

To share the database with the Python service, point both at the same PostgreSQL
instance; the .NET entities are mapped to the identical snake_case tables.

## Migrations

```bash
cd src/SicData.Api
dotnet ef migrations add <Name>
dotnet ef database update
```

## Tests

```bash
dotnet test
```

Covers the full dataset lifecycle, duplicate-name conflict, company pagination +
county filter, PATCH cell editing, analysis computation, cross-dataset search,
CSV export, and 404 handling.
