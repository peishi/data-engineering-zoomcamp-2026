import pandas as pd
from sqlalchemy import create_engine

path = '01-docker-terraform/homework/'

# wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet
df = pd.read_parquet(f'{path}/green_tripdata_2025-11.parquet')

# wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
zones = pd.read_csv(f'{path}/taxi_zone_lookup.csv')

conn = create_engine(f'postgresql://root:root@localhost:5432/ny_taxi')

print('Schema:')
print(pd.io.sql.get_schema(df, name='green_taxi_data', con=conn))

df.to_sql(
    name='green_taxi',
    con=conn, 
    if_exists='replace'
)
zones.to_sql(
    name='zones',
    con=conn, 
    if_exists='replace'
)