# PG Drift

A Python tool for detecting and analyzing schema differences between PostgreSQL databases.

PG Drift exports database metadata, generates checksums for comparison, and produces detailed reports that displays schema differences.

## Installation

### Prerequisites

- Python 3.9.32 or higher. (Project developed on Python 3.9.32)
- Network access to target PostgreSQL databases

### Setup

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd PG-Drift
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

PG Drift uses environment variables for configuration. Set the following variables before running:

### Required Environment Variables

```bash
# Optional, output folder for reports (default: "pgdrift-output")
export PG_DRIFT_TARGET_FOLDER="pgdrift-output"

# Number of databases to compare
export DB_COUNT=3

# Database 1 configuration
export PG_DRIFT_DB_HOST_1="localhost"
export PG_DRIFT_DB_PORT_1="5432"
export PG_DRIFT_DB_USER_1="postgres"
export PG_DRIFT_DB_PASSWORD_1="your_password"
export PG_DRIFT_DB_NAME_1="database1"

# Database 2 configuration
export PG_DRIFT_DB_HOST_2="localhost"
export PG_DRIFT_DB_PORT_2="5432"
export PG_DRIFT_DB_USER_2="postgres"
export PG_DRIFT_DB_PASSWORD_2="your_password"
export PG_DRIFT_DB_NAME_2="database2"

# Database 3 configuration
export PG_DRIFT_DB_HOST_3="localhost"
export PG_DRIFT_DB_PORT_3="5432"
export PG_DRIFT_DB_USER_3="postgres"
export PG_DRIFT_DB_PASSWORD_3="your_password"
export PG_DRIFT_DB_NAME_3="database3"

# Add more as needed...
```

## Usage

### Basic Usage

Navigate to the `src` directory and run:

```bash
cd src
python3 main.py
```

## Output Files

PG Drift generates several files in the output folder (default: `src/pgdrift-output/`):

| File Name | Description |
|-------------|-------------|
| `{timestamp}-db_X-{dbname}.json` | Raw metadata export for each database |
| `{timestamp}-checksums.csv` | SHA-256 checksums for each database |
| `{timestamp}-checksum_match_matrix.csv` | Matrix showing which databases match/mismatch |
| `{timestamp}-schema_differences.csv` | Detailed diff report (only if differences are detected) |

## Related

I bootstrapped an [AWS Infrastructure using Terraform](https://github.com/mandyteo/PG-Drift-Test-IaC) to test this project.