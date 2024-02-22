{{ config(materialized='view') }}

with tripdata as 
(
  select *,
  from {{ source('staging','fhv_trips') }}
)
select
    -- identifiers
    cast(PUlocationID as numeric) as  pickup_locationid,
    cast(DOlocationID as numeric) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    SR_Flag as store_and_fwd_flag,

from tripdata
where pickup_datetime between "2019-01-01" and "2019-12-31"

-- dbt build --e <model.sql> --var 'is_test_run: false'

{% if var('is_test_run', default=true) %}

    limit 100
    
{% endif %}
