{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
)

trips_unioned as (
    SELECT * FROM fhv_data
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    trips_unioned.pickup_locationid, 
    trips_unioned.dropoff_locationid, 
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.store_and_fwd_flag
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
