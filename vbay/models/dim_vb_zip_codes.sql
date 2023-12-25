with stg_vb_zip_codes as (
    select * from {{ source('vbay', 'vb_zip_codes')}}
)

select 
{{ dbt_utils.generate_surrogate_key(['stg_vb_zip_codes.zip_code'])}} as zipcodekey, 
stg_vb_zip_codes.* 
from stg_vb_zip_codes