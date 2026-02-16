"""
NYC Taxi Data Pipeline using Prefect
Orchestrates downloading, processing, and storing NYC TLC trip record data
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
import pandas as pd
import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.artifacts import create_table_artifact


# Configuration
DATA_DIR = Path("data/nyc_taxi")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


@task(
    name="download_taxi_data",
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1)
)
def download_taxi_data(
    year: int,
    month: int,
    taxi_type: str = "yellow"
) -> Optional[Path]:
    """
    Download NYC taxi data for a specific year and month.
    
    Args:
        year: Year of the data (e.g., 2024)
        month: Month of the data (1-12)
        taxi_type: Type of taxi data ('yellow', 'green', 'fhv', 'fhvhv')
    
    Returns:
        Path to downloaded file or None if download failed
    """
    # Format month with leading zero
    month_str = f"{month:02d}"
    
    # Construct URL
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    filename = f"{taxi_type}_tripdata_{year}-{month_str}.parquet"
    url = f"{base_url}/{filename}"
    
    output_path = RAW_DIR / filename
    
    # Skip if already downloaded
    if output_path.exists():
        print(f"File already exists: {filename}")
        return output_path
    
    try:
        print(f"Downloading {filename}...")
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Download with progress
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    if total_size > 0:
                        progress = (downloaded_size / total_size) * 100
                        if downloaded_size % (1024 * 1024 * 10) == 0:  # Log every 10MB
                            print(f"Progress: {progress:.1f}%")
        
        print(f"Successfully downloaded {filename} ({downloaded_size / 1024 / 1024:.2f} MB)")
        return output_path
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {filename}: {e}")
        return None


@task(name="validate_data")
def validate_data(file_path: Path, taxi_type: str = "yellow") -> bool:
    """
    Validate the downloaded taxi data.
    
    Args:
        file_path: Path to the parquet file
        taxi_type: Type of taxi data for column validation
    
    Returns:
        True if validation passes, False otherwise
    """
    try:
        df = pd.read_parquet(file_path)
        
        # Check if dataframe is not empty
        if df.empty:
            print(f"Validation failed: {file_path.name} is empty")
            return False
        
        # Define expected columns for each taxi type
        expected_columns = {
            "yellow": ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime"],
            "green": ["VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime"],
            "fhv": ["pickup_datetime", "dropOff_datetime"],
            "fhvhv": ["hvfhs_license_num", "pickup_datetime", "dropoff_datetime"]
        }
        
        # Check for key columns
        key_cols = expected_columns.get(taxi_type, [])
        missing_cols = [col for col in key_cols if col not in df.columns]
        
        if missing_cols:
            print(f"Validation warning: Missing expected columns: {missing_cols}")
        
        print(f"Validation passed: {file_path.name} ({len(df):,} records)")
        return True
        
    except Exception as e:
        print(f"Validation error for {file_path.name}: {e}")
        return False


@task(name="process_taxi_data")
def process_taxi_data(file_path: Path, taxi_type: str = "yellow") -> Optional[Path]:
    """
    Process and clean taxi data.
    
    Args:
        file_path: Path to raw parquet file
        taxi_type: Type of taxi data
    
    Returns:
        Path to processed file or None if processing failed
    """
    try:
        print(f"Processing {file_path.name}...")
        df = pd.read_parquet(file_path)
        
        initial_count = len(df)
        
        # Process based on taxi type
        if taxi_type == "yellow":
            # Remove invalid trips
            df = df[
                (df['trip_distance'] > 0) &
                (df['fare_amount'] > 0) &
                (df['tpep_pickup_datetime'] < df['tpep_dropoff_datetime'])
            ]
            
            # Calculate trip duration
            df['trip_duration_minutes'] = (
                df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
            ).dt.total_seconds() / 60
            
            # Filter reasonable trip durations (0-180 minutes)
            df = df[(df['trip_duration_minutes'] > 0) & (df['trip_duration_minutes'] < 180)]
            
        elif taxi_type == "green":
            df = df[
                (df['trip_distance'] > 0) &
                (df['fare_amount'] > 0) &
                (df['lpep_pickup_datetime'] < df['lpep_dropoff_datetime'])
            ]
            
            df['trip_duration_minutes'] = (
                df['lpep_dropoff_datetime'] - df['lpep_pickup_datetime']
            ).dt.total_seconds() / 60
            
            df = df[(df['trip_duration_minutes'] > 0) & (df['trip_duration_minutes'] < 180)]
        
        final_count = len(df)
        removed_count = initial_count - final_count
        
        print(f"Cleaned data: removed {removed_count:,} invalid records ({removed_count/initial_count*100:.2f}%)")
        
        # Save processed data
        output_path = PROCESSED_DIR / file_path.name.replace('.parquet', '_processed.parquet')
        df.to_parquet(output_path, index=False)
        
        print(f"Processed file saved: {output_path.name} ({len(df):,} records)")
        return output_path
        
    except Exception as e:
        print(f"Processing error for {file_path.name}: {e}")
        return None


@task(name="generate_summary_stats")
def generate_summary_stats(file_path: Path, taxi_type: str = "yellow") -> dict:
    """
    Generate summary statistics for the processed data.
    
    Args:
        file_path: Path to processed parquet file
        taxi_type: Type of taxi data
    
    Returns:
        Dictionary of summary statistics
    """
    try:
        df = pd.read_parquet(file_path)
        
        stats = {
            "file_name": file_path.name,
            "total_trips": len(df),
            "date_range": f"{df.iloc[0]['tpep_pickup_datetime' if taxi_type == 'yellow' else 'lpep_pickup_datetime'].date()} to "
                         f"{df.iloc[-1]['tpep_pickup_datetime' if taxi_type == 'yellow' else 'lpep_pickup_datetime'].date()}",
            "avg_trip_distance": round(df['trip_distance'].mean(), 2),
            "avg_fare_amount": round(df['fare_amount'].mean(), 2),
            "total_revenue": round(df['total_amount'].sum(), 2) if 'total_amount' in df.columns else 0,
        }
        
        if 'trip_duration_minutes' in df.columns:
            stats["avg_trip_duration_min"] = round(df['trip_duration_minutes'].mean(), 2)
        
        # Create Prefect artifact with summary table
        create_table_artifact(
            key=f"summary-stats-{file_path.stem}",
            table=[stats],
            description=f"Summary statistics for {file_path.name}"
        )
        
        print(f"Summary stats generated for {file_path.name}")
        return stats
        
    except Exception as e:
        print(f"Error generating stats for {file_path.name}: {e}")
        return {}


@flow(name="ingest_monthly_taxi_data", log_prints=True)
def ingest_monthly_taxi_data(
    year: int,
    month: int,
    taxi_type: str = "yellow",
    skip_processing: bool = False
) -> dict:
    """
    Flow to ingest and process NYC taxi data for a single month.
    
    Args:
        year: Year of data
        month: Month of data (1-12)
        taxi_type: Type of taxi ('yellow', 'green', 'fhv', 'fhvhv')
        skip_processing: If True, only download without processing
    
    Returns:
        Dictionary with pipeline results
    """
    print(f"\n{'='*60}")
    print(f"Starting NYC Taxi Data Pipeline")
    print(f"Taxi Type: {taxi_type}")
    print(f"Period: {year}-{month:02d}")
    print(f"{'='*60}\n")
    
    # Download data
    raw_file = download_taxi_data(year, month, taxi_type)
    
    if raw_file is None:
        return {
            "status": "failed",
            "reason": "Download failed",
            "year": year,
            "month": month
        }
    
    # Validate data
    is_valid = validate_data(raw_file, taxi_type)
    
    if not is_valid:
        return {
            "status": "failed",
            "reason": "Validation failed",
            "year": year,
            "month": month
        }
    
    if skip_processing:
        return {
            "status": "success",
            "stage": "download_only",
            "raw_file": str(raw_file)
        }
    
    # Process data
    processed_file = process_taxi_data(raw_file, taxi_type)
    
    if processed_file is None:
        return {
            "status": "partial_success",
            "reason": "Processing failed",
            "raw_file": str(raw_file)
        }
    
    # Generate statistics
    stats = generate_summary_stats(processed_file, taxi_type)
    
    return {
        "status": "success",
        "year": year,
        "month": month,
        "taxi_type": taxi_type,
        "raw_file": str(raw_file),
        "processed_file": str(processed_file),
        "stats": stats
    }


@flow(name="ingest_taxi_data_batch", log_prints=True)
def ingest_taxi_data_batch(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    taxi_type: str = "yellow"
) -> List[dict]:
    """
    Flow to ingest NYC taxi data for multiple months.
    
    Args:
        start_year: Starting year
        start_month: Starting month
        end_year: Ending year
        end_month: Ending month
        taxi_type: Type of taxi data
    
    Returns:
        List of results for each month
    """
    results = []
    
    current_date = datetime(start_year, start_month, 1)
    end_date = datetime(end_year, end_month, 1)
    
    while current_date <= end_date:
        result = ingest_monthly_taxi_data(
            year=current_date.year,
            month=current_date.month,
            taxi_type=taxi_type
        )
        results.append(result)
        
        # Move to next month
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)
    
    # Summary
    successful = sum(1 for r in results if r.get("status") == "success")
    print(f"\n{'='*60}")
    print(f"Batch Pipeline Summary")
    print(f"Total months processed: {len(results)}")
    print(f"Successful: {successful}")
    print(f"Failed: {len(results) - successful}")
    print(f"{'='*60}\n")
    
    return results


if __name__ == "__main__":
    # Example usage: Download and process yellow taxi data for January 2024
    result = ingest_monthly_taxi_data(
        year=2024,
        month=1,
        taxi_type="yellow"
    )
    
    print("\n" + "="*60)
    print("Pipeline Result:")
    print("="*60)
    for key, value in result.items():
        print(f"{key}: {value}")
    
    # Example: Batch processing for multiple months
    # Uncomment to run batch processing
    # batch_results = ingest_taxi_data_batch(
    #     start_year=2024,
    #     start_month=1,
    #     end_year=2024,
    #     end_month=3,
    #     taxi_type="yellow"
    # )
