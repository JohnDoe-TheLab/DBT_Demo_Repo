{{config(
        materialized = 'incremental'
        ,tags = ["curation","orders"]
        ,unique_key = 'O_ORDERKEY'
        )
}}

--imports CTE

with orders_delta as 
(
    select * from {{ref('Order_Delta')}}
)

--logic CTE

--Final CTE

--Final Select