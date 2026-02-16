# NYC Taxi Data Pipeline with Prefect

A robust, production-ready data pipeline for ingesting and processing NYC TLC (Taxi & Limousine Commission) trip record data using Prefect for orchestration.

## Features

- **Automated Data Download**: Downloads NYC taxi trip data from official sources
- **Data Validation**: Validates downloaded data for completeness and correctness
- **Data Processing**: Cleans and processes raw data (removes invalid trips, calculates metrics)
- **Batch Processing**: Support for processing multiple months of data
- **Error Handling**: Automatic retries, error logging, and graceful failure handling
- **Caching**: Intelligent caching to avoid re-downloading existing files
- **Summary Statistics**: Generates comprehensive summary statistics
- **Prefect Artifacts**: Creates visual artifacts in Prefect UI for monitoring

## Installation

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Prefect Server (Optional but Recommended)

For the full Prefect UI experience:

```bash
# Start Prefect server
prefect server start
```

Then open your browser to `http://127.0.0.1:4200` to access the Prefect UI.

## Usage

### Basic Usage - Single Month

Download and process yellow taxi data for a specific month:

```python
from nyc_taxi_pipeline import ingest_monthly_taxi_data

# Download and process January 2024 data
result = ingest_monthly_taxi_data(
    year=2024,
    month=1,
    taxi_type="yellow"
)
```

### Command Line Execution

```bash
python nyc_taxi_pipeline.py
```

### Batch Processing - Multiple Months

Process several months of data in sequence:

```python
from nyc_taxi_pipeline import ingest_taxi_data_batch

# Process Q1 2024 data
results = ingest_taxi_data_batch(
    start_year=2024,
    start_month=1,
    end_year=2024,
    end_month=3,
    taxi_type="yellow"
)
```

### Different Taxi Types

The pipeline supports multiple taxi types:

```python
# Yellow taxis (most common)
ingest_monthly_taxi_data(2024, 1, taxi_type="yellow")

# Green taxis
ingest_monthly_taxi_data(2024, 1, taxi_type="green")

# For-Hire Vehicles (FHV)
ingest_monthly_taxi_data(2024, 1, taxi_type="fhv")

# High Volume For-Hire Vehicles (FHVHV) - e.g., Uber, Lyft
ingest_monthly_taxi_data(2024, 1, taxi_type="fhvhv")
```

### Download Only (Skip Processing)

```python
result = ingest_monthly_taxi_data(
    year=2024,
    month=1,
    taxi_type="yellow",
    skip_processing=True
)
```

## Pipeline Architecture

### Tasks

1. **download_taxi_data**: Downloads parquet files from NYC TLC CloudFront CDN
   - Retries: 3 attempts with 60s delay
   - Caching: 24-hour cache to avoid re-downloads
   
2. **validate_data**: Validates downloaded data
   - Checks for empty dataframes
   - Verifies expected columns exist
   
3. **process_taxi_data**: Cleans and enriches data
   - Removes invalid trips (negative distances, fares)
   - Filters impossible trip durations
   - Calculates trip duration in minutes
   
4. **generate_summary_stats**: Creates summary statistics
   - Total trips, revenue, averages
   - Creates Prefect artifacts for visualization

### Flows

1. **ingest_monthly_taxi_data**: Main flow for single month
2. **ingest_taxi_data_batch**: Batch flow for multiple months

## Data Storage

```
data/
└── nyc_taxi/
    ├── raw/              # Raw downloaded parquet files
    └── processed/        # Cleaned and processed parquet files
```

## Configuration

Edit these variables in the script to customize:

```python
DATA_DIR = Path("data/nyc_taxi")  # Base data directory
RAW_DIR = DATA_DIR / "raw"         # Raw data location
PROCESSED_DIR = DATA_DIR / "processed"  # Processed data location
```

## Monitoring with Prefect

### View Flow Runs

1. Start Prefect server: `prefect server start`
2. Navigate to http://127.0.0.1:4200
3. View flow runs, task execution times, and artifacts

### Create a Deployment

For scheduled execution:

```bash
# Create a deployment
prefect deployment build nyc_taxi_pipeline.py:ingest_monthly_taxi_data \
    -n "Monthly Yellow Taxi Ingestion" \
    -q default

# Apply the deployment
prefect deployment apply ingest_monthly_taxi_data-deployment.yaml

# Start a worker
prefect worker start -q default
```

### Schedule Regular Ingestion

```python
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

deployment = Deployment.build_from_flow(
    flow=ingest_monthly_taxi_data,
    name="monthly-taxi-ingestion",
    schedule=CronSchedule(cron="0 2 1 * *"),  # 2 AM on 1st of each month
    parameters={
        "year": 2024,
        "month": 1,
        "taxi_type": "yellow"
    }
)

deployment.apply()
```

## Data Processing Details

### Yellow/Green Taxi Processing

The pipeline removes:
- Trips with zero or negative distance
- Trips with zero or negative fare
- Trips where pickup time >= dropoff time
- Trips with duration > 180 minutes (likely data errors)

Calculated fields:
- `trip_duration_minutes`: Duration in minutes

### Output Statistics

For each processed file, the pipeline generates:
- Total trips
- Date range
- Average trip distance
- Average fare amount
- Total revenue
- Average trip duration

## Troubleshooting

### Download Failures

If downloads fail:
- Check internet connection
- Verify the month/year combination exists
- Check if NYC TLC has published data for that period

### Memory Issues

For large datasets:
- Process files individually instead of batch mode
- Consider using Dask for larger-than-memory processing

### Prefect Server Issues

If Prefect UI isn't accessible:
```bash
# Reset Prefect database
prefect database reset

# Restart server
prefect server start
```

## Data Source

Data is sourced from NYC Taxi & Limousine Commission (TLC):
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Data is stored on AWS CloudFront CDN and updated monthly.

## License

This pipeline is for educational and research purposes. Please refer to NYC TLC's terms of use for the data itself.

## Future Enhancements

- [ ] Add support for direct database loading (PostgreSQL, BigQuery)
- [ ] Implement incremental loading
- [ ] Add data quality metrics and alerting
- [ ] Create data visualizations and dashboards
- [ ] Add support for spatial analysis with pickup/dropoff locations
- [ ] Integration with dbt for transformation
