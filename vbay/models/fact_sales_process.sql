with stg_users as (
  select user_id, user_email, user_firstname, user_lastname, user_zip_code
  from {{source('vbay','vb_users')}}
) , 

stg_zip_codes as (
    select zip_code, zip_city, zip_state, zip_lat, zip_lng 
    from {{source('vbay', 'vb_zip_codes')}}
) , 

stg_items as (
    select item_id, item_name, item_type, item_description, 
    item_reserve, item_enddate, item_sold, item_seller_user_id, 
    item_buyer_user_id, item_soldamount
    from {{source('vbay', 'vb_items')}}
) 
, 

stg_users_zipcodes_items as (
    select i.item_id, i.item_name, i.item_type, i.item_description, 
    i.item_reserve, date(i.item_enddate) as item_enddate, i.item_buyer_user_id, u.user_email, u.user_zip_code, 
    z.zip_city, z.zip_state, z.zip_lat, z.zip_lng 
    from stg_items i 
    join stg_users u on i.item_buyer_user_id = u.user_id
    left join stg_zip_codes z on u.user_zip_code = z.zip_code
    )

select count(item_id) as items_purchased, item_buyer_user_id as user_id, user_email, user_zip_code, 
zip_city, zip_state
from stg_users_zipcodes_items
group by item_buyer_user_id, user_email, user_zip_code, zip_city, zip_state
order by item_buyer_user_id


