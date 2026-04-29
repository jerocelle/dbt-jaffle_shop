/*
-- Tested storage configuration in the model level. This will override folder level configuration.
{{ 
    config(
        schema='staging'
    )
}}
*/


with 

source as (

    select * from {{ source('jaffle_shop', 'customers') }}
  
),

renamed as (
   
   select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed
