with tripdata as (
  select *
  from {{ source('raw_data','fhv_tripdata') }}
),

renamed as (
  select
    -- identifiers
    dispatching_base_num,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,
    affiliated_base_number as affiliated_base_num,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    sr_flag

  from tripdata
  WHERE dispatching_base_num IS NOT NULL
)

select * from renamed