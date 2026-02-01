import sys
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# day = int(sys.argv[1])
# print(f"Running pipeline for day {day}")

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

    
@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    df_iter = pd.read_csv(
        prefix + 'yellow_tripdata_2021-01.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000
    )

    conn = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    # Schema
    # print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name=target_table,
        con=conn,
        if_exists="replace"
    )

    print("Table created")

    first_chunk.to_sql(
        name=target_table,
        con=conn,
        if_exists="append"
    )

    print("Inserted first chunk:", len(first_chunk))

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=conn,
            if_exists="append"
        )
        print("Inserted chunk:", len(df_chunk))