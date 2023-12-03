with f_sales as (
    select * from {{ ref('fact_sales') }}
) , 

d_date as (
    select * from {{ ref('dim_date') }}
)

select f_sales.*, d_date.* 
from f_sales LEFT JOIN d_date on f_sales.orderdatekey = d_date.datekey
