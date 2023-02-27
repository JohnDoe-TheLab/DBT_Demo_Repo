{{ config(schema='CUR') }}

with customers as
(
    Select
    c_custkey
    ,c_name
    ,c_address
    ,c_acctbal
    ,c_mktsegment
    from dbt_data_eng_demo.raw.raw_customer_america

    Union all

    Select
    c_custkey
    ,c_name
    ,c_address
    ,c_acctbal
    ,c_mktsegment
    from dbt_data_eng_demo.raw.raw_customer_asia
)

select * from customers