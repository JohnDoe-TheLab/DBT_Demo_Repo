--This is an example of an incremental load using an ETL/ELT datetime stamp

{{config(
        materialized = 'incremental'
        ,tags = ["curation","orders","lines"]
        ,unique_key = ['l_orderkey','l_linenumber']
        ,schema='CUR'
        )
}}

with cte_lineItems as 
(
    select * from {{ref('LineItems')}}
)

Select
*
from cte_lineItems

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where AUDIT_DATETIME > (select max(AUDIT_DATETIME) from {{ this }})

{% endif %}