with f_location_analysis as (
    select * from {{ ref('fact_location_analysis')}}
)

select * from f_location_analysis

