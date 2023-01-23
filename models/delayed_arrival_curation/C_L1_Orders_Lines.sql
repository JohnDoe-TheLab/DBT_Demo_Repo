--This is an example of an incremental load using an ETL/ELT datetime stamp

{{config(
        materialized = 'table'
        ,tags = ["curation","orders","lines"]
        ,unique_key = 'O_LINEKEY'
        ,schema='CUR'
        )
}}

with cte_orders_lines as 
(
    select * from {{ref('Orders_Lines')}}
)

Select
*
from cte_orders_lines

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where AUDIT_DATETIME > (select max(AUDIT_DATETIME) from {{ this }})

{% endif %}