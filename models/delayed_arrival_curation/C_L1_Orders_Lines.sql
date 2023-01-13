{{config(
        materialized = 'incremental'
        ,tags = ["curation","orders","lines"]
        ,unique_key = 'O_LINEKEY'
        ,schema='CUR'
        )
}}

with cte_orders_lines as 
(
    select * from {{ref('Orders_lines')}}
)

Select
*
from cte_orders_lines