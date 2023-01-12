{{config(
        materialized = 'incremental'
        ,tags = ["curation","orders"]
        ,unique_key = 'O_ORDERKEY'
        ,schema='CUR'
        )
}}

--Delta CTE

with cte_orders_delta as 
(
    select * from {{ref('Orders_Delta')}}
)

--Base CTE
, cte_orders_base as
(
    Select  
        AUDIT_DATETIME
        ,DELETE_FLAG
        ,O_ORDERKEY
        ,O_CUSTKEY
        ,O_NATIONKEY
        ,O_REGIONKEY
        ,O_TOTALPRICE
        ,O_ORDERDATE
        ,O_CREATE_DATETIME
        ,O_UPDATE_DATETIME
    FROM {{ref('Orders')}}
)

--Final CTE

--Final Select
Select
*
from cte_orders_base B
{% if is_incremental() %}
 where not EXISTS
 (
     select 
     O_ORDERKEY
     from cte_orders_delta D
     where D.O_ORDERKEY = B.O_ORDERKEY
 ) 
 {% endif %}