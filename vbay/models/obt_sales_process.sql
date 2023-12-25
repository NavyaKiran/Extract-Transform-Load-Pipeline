with f_sales_process as (
    select * from {{ ref('fact_sales_process')}}
)

select * from f_sales_process

