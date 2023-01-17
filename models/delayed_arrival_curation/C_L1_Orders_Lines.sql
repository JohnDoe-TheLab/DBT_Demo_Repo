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