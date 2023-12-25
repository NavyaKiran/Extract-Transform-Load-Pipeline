with stg_users as (
  select user_id, user_email, user_firstname, user_lastname, user_zip_code
  from {{source('vbay','vb_users')}}
) , 

stg_zip_codes as (
    select zip_code, zip_city, zip_state, zip_lat, zip_lng 
    from {{source('vbay', 'vb_zip_codes')}}
) , 

stg_users_zip as (
select u.user_id, u.user_email, u.user_firstname, u.user_lastname, u.user_zip_code,
z.zip_city, z.zip_state, z.zip_lat, z.zip_lng 
from stg_users u
left join stg_zip_codes z on u.user_zip_code = z.zip_code)

select count(user_id) as number_of_users, user_zip_code, zip_city, zip_state, zip_lat, zip_lng
from stg_users_zip 
group by user_zip_code, zip_city, zip_state, zip_lat, zip_lng 
order by user_zip_code


