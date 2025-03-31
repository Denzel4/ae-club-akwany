{{ config(
    materialized='table'
)}}

with daily_popular_flavor as (

    select 

    date(dbt_valid_from) as flavor_date,
    favorite_ice_cream_flavor,
    count(*) as count_flavor
    from {{ ref('favorite_ice_cream_flavors') }}
    where dbt_valid_to is NULL
    group by 1,2

),
rank_flavor as (
    select 
    flavor_date,
    favorite_ice_cream_flavor,
    count_flavor,
    rank() over ( partition by flavor_date order by count_flavor desc) as rank
    from daily_popular_flavor


)
select 

    * 
from rank_flavor
order by flavor_date , rank desc

