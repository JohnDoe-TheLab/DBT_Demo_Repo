{{config(
        materialized = 'incremental'
        ,tags = ["curation","orders"]
        ,unique_key = 'O_ORDERKEY'
        )
}}

--imports CTE

with cte_orders_delta as 
(
    --select * from {{ref('Order_Delta')}}
    select * from LATE_ARRIVING_DEMO.RAW.STM_ORDERS_AMERICA_DELTA
)

--logic CTE

--Final CTE

--Final Select

Select * from cte_orders_delta