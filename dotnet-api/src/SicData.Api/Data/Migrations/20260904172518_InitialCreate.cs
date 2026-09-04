using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace SicData.Api.Data.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "datasets",
                columns: table => new
                {
                    id = table.Column<int>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    name = table.Column<string>(type: "TEXT", maxLength: 255, nullable: false),
                    description = table.Column<string>(type: "TEXT", nullable: true),
                    sic_codes = table.Column<string>(type: "TEXT", nullable: false),
                    counties = table.Column<string>(type: "TEXT", nullable: true),
                    total_companies = table.Column<int>(type: "INTEGER", nullable: false),
                    source_file = table.Column<string>(type: "TEXT", maxLength: 500, nullable: true),
                    created_at = table.Column<DateTime>(type: "TEXT", nullable: true),
                    updated_at = table.Column<DateTime>(type: "TEXT", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_datasets", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "companies",
                columns: table => new
                {
                    id = table.Column<int>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    dataset_id = table.Column<int>(type: "INTEGER", nullable: false),
                    company_number = table.Column<string>(type: "TEXT", maxLength: 8, nullable: false),
                    business_name = table.Column<string>(type: "TEXT", nullable: false),
                    address_line1 = table.Column<string>(type: "TEXT", nullable: true),
                    address_line2 = table.Column<string>(type: "TEXT", nullable: true),
                    town = table.Column<string>(type: "TEXT", nullable: true),
                    county = table.Column<string>(type: "TEXT", maxLength: 100, nullable: true),
                    postcode = table.Column<string>(type: "TEXT", maxLength: 10, nullable: true),
                    person_with_significant_control = table.Column<string>(type: "TEXT", nullable: true),
                    nature_of_control = table.Column<string>(type: "TEXT", nullable: true),
                    title = table.Column<string>(type: "TEXT", maxLength: 50, nullable: true),
                    fname = table.Column<string>(type: "TEXT", maxLength: 100, nullable: true),
                    sname = table.Column<string>(type: "TEXT", maxLength: 100, nullable: true),
                    selected_person_source = table.Column<string>(type: "TEXT", nullable: true),
                    selected_psc_share_tier = table.Column<string>(type: "TEXT", maxLength: 20, nullable: true),
                    selected_psc_nature_of_control = table.Column<string>(type: "TEXT", nullable: true),
                    position = table.Column<string>(type: "TEXT", nullable: true),
                    sic = table.Column<string>(type: "TEXT", nullable: true),
                    company_status = table.Column<string>(type: "TEXT", maxLength: 50, nullable: true),
                    company_type = table.Column<string>(type: "TEXT", maxLength: 100, nullable: true),
                    date_of_creation = table.Column<string>(type: "TEXT", maxLength: 20, nullable: true),
                    website = table.Column<string>(type: "TEXT", nullable: true),
                    phone = table.Column<string>(type: "TEXT", maxLength: 50, nullable: true),
                    email = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    website_address = table.Column<string>(type: "TEXT", nullable: true),
                    address_match = table.Column<string>(type: "TEXT", maxLength: 50, nullable: true),
                    created_at = table.Column<DateTime>(type: "TEXT", nullable: true),
                    updated_at = table.Column<DateTime>(type: "TEXT", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_companies", x => x.id);
                    table.ForeignKey(
                        name: "FK_companies_datasets_dataset_id",
                        column: x => x.dataset_id,
                        principalTable: "datasets",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "dataset_analysis",
                columns: table => new
                {
                    id = table.Column<int>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    dataset_id = table.Column<int>(type: "INTEGER", nullable: false),
                    total_companies = table.Column<int>(type: "INTEGER", nullable: false),
                    unique_counties = table.Column<int>(type: "INTEGER", nullable: false),
                    data_quality_score = table.Column<double>(type: "REAL", nullable: false),
                    regional_distribution = table.Column<string>(type: "TEXT", nullable: false),
                    county_resolution = table.Column<string>(type: "TEXT", nullable: false),
                    missing_data = table.Column<string>(type: "TEXT", nullable: false),
                    generated_at = table.Column<DateTime>(type: "TEXT", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_dataset_analysis", x => x.id);
                    table.ForeignKey(
                        name: "FK_dataset_analysis_datasets_dataset_id",
                        column: x => x.dataset_id,
                        principalTable: "datasets",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_companies_company_number",
                table: "companies",
                column: "company_number");

            migrationBuilder.CreateIndex(
                name: "IX_companies_county",
                table: "companies",
                column: "county");

            migrationBuilder.CreateIndex(
                name: "IX_companies_dataset_id_county",
                table: "companies",
                columns: new[] { "dataset_id", "county" });

            migrationBuilder.CreateIndex(
                name: "IX_companies_postcode",
                table: "companies",
                column: "postcode");

            migrationBuilder.CreateIndex(
                name: "IX_dataset_analysis_dataset_id",
                table: "dataset_analysis",
                column: "dataset_id",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_datasets_name",
                table: "datasets",
                column: "name",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "companies");

            migrationBuilder.DropTable(
                name: "dataset_analysis");

            migrationBuilder.DropTable(
                name: "datasets");
        }
    }
}
