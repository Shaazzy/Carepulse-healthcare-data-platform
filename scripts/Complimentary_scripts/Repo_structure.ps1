
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
#update the folder structure

# CarePulse production-style repository scaffolding
# Run from the GitHub repository root.

$folders = @(
    ".github/workflows",
    "config/dev",
    "config/prod",
    "docker",
    "src/carepulse",
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

$files = @(
    "README.md",
    ".gitignore",
    ".env.example",
    "pyproject.toml",
    "docker-compose.yml",
    "src/carepulse/__init__.py",
    "src/carepulse/generators/__init__.py",
    "src/carepulse/producers/__init__.py",
    "src/carepulse/consumers/__init__.py",
    "src/carepulse/ingestion/__init__.py",
    "src/carepulse/common/__init__.py",
    "src/carepulse/quality/__init__.py",
    "src/carepulse/sqlserver/__init__.py"
)

foreach ($file in $files) {
    if (-not (Test-Path $file)) {
        New-Item -ItemType File -Force -Path $file |
            Out-Null
    }
}

Write-Host "CarePulse repository structure ready." `
    -ForegroundColor Green
```

## 4. Add the production engineering standards

These are the standards I want us to implement as we build the project.

| Area | Production-grade requirement |
|---|---|
| Python | Reusable modules, type hints, structured logging |
| Kafka | Stable patient keys, delivery confirmation, retries, consumer groups |
| Ingestion | Batch files, durable writes, offset management, replay support |
| SQL Server | Parameterized SQL, migrations, connection management |
| ADLS Gen2 | Organized partitions, access controls, lifecycle policies |
| Databricks | Incremental processing, deduplication, schema enforcement |
| Data quality | Null checks, uniqueness, referential integrity, range validation |
| ADF | Parameterization, retries, failure handling, monitoring |
| Security | Managed identities, Key Vault, no committed credentials |
| CI/CD | Linting, unit tests, integration tests, deployment checks |
| Infrastructure | Terraform, environment separation, repeatable provisioning |
| Operations | Logs, alerting, runbooks, recovery procedures |

### An important Kafka design decision

For your Kafka-to-ADLS consumer, I recommend implementing **at-least-once delivery with idempotent processing**, rather than claiming exactly-once delivery.

That means:

1. Consume Kafka records in batches.
2. Write the batch to durable ADLS storage.
3. Commit Kafka offsets only after successful upload.
4. Handle retries and consumer rebalances.
5. Deduplicate replayed events downstream using `EventID` or Kafka topic, partition, and offset.

This is a realistic, defensible engineering design for your portfolio.

## 5. Add a proper Python project configuration

Create `pyproject.toml`:

<CodeBlock language="toml" editable>
[build-system]
requires = ["setuptools>=69"]
build-backend = "setuptools.build_meta"

[project]
name = "carepulse-healthcare-data-platform"
version = "0.1.0"
description = "Healthcare batch and streaming data platform"
requires-python = ">=3.11,<3.13"

dependencies = [
    "pandas>=2.2,<3",
    "faker>=30,<40",
    "Pillow>=11,<13",
    "confluent-kafka>=2.5,<3",
    "azure-identity>=1.19,<2",
    "azure-storage-file-datalake>=12.17,<13",
    "python-dotenv>=1,<2",
    "pyodbc>=5,<6"
]

[project.optional-dependencies]
dev = [
    "pytest>=8,<10",
    "pytest-cov>=5,<8",
    "ruff>=0.9,<1"
]

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py311"
