with stg_products as 
(
    select 
        productid, productname
        from {{source('northwind','Products')}}
), 

stg_orders as
(
    select replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey, orderid
    from {{source('northwind','Orders')}} 
),

stg_order_details as
(
    select a.orderid, a.productid, stg_products.productname, 
    sum(a.Quantity) as quantityonorder, 
    sum(a.UnitPrice * a.Quantity * a.Discount) as discountamount, 
    sum(a.Quantity*a.UnitPrice*(1-a.Discount)) as soldamount
    from {{source('northwind','Order_Details')}} a left join stg_products on a.productid = stg_products.productid
    group by a.orderid, a.productid, stg_products.productname
)

select stg_order_details.*, stg_orders.orderdatekey 
from stg_order_details JOIN stg_orders on stg_order_details.orderid = stg_orders.orderid
