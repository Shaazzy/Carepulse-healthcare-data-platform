
# CarePulse Production-Grade Repository Structure

$folders = @(
    ".github/workflows",
    "config/dev",
    "config/prod",
    "docker",
    "src/carepulse/generators",
    "src/carepulse/producers",
    "src/carepulse/consumers",
    "src/carepulse/ingestion",
    "src/carepulse/sqlserver",
    "src/carepulse/schemas",
    "src/carepulse/quality",
    "src/carepulse/common",
    "databricks/bronze",
    "databricks/silver",
    "databricks/gold",
    "adf/pipeline",
    "adf/dataset",
    "adf/linkedService",
    "infrastructure/terraform",
    "sql/ddl",
    "sql/queries",
    "powerbi",
    "tests/unit",
    "tests/integration",
    "tests/data_quality",
    "docs/architecture",
    "docs/runbooks",
    "docs/screenshots",
    "notebooks",
    "data/sample",
    "scripts"
)

foreach ($folder in $folders) {
    New-Item -ItemType Directory -Force -Path $folder |
        Out-Null

    $keep = Join-Path $folder ".gitkeep"

    if (-not (Test-Path $keep)) {
        New-Item -ItemType File -Path $keep |
            Out-Null
    }
}

Write-Host "CarePulse folders created successfully!" -ForegroundColor Green
