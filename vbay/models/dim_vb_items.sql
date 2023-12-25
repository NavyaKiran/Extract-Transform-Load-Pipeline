with stg_vb_items as (
    select * from {{ source('vbay', 'vb_items')}}
)

select {{ dbt_utils.generate_surrogate_key(['stg_vb_items.item_id'])}} as itemkey,
stg_vb_items.*
from stg_vb_items
