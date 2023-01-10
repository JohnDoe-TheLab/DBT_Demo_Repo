with customers as
(
    Select
    c_custkey
    ,c_name
    ,c_address
    ,c_acctbal
    ,c_mktsegment
    from late_arriving_demo.raw.raw_customer_america

    Union all

    Select
    c_custkey
    ,c_name
    ,c_address
    ,c_acctbal
    ,c_mktsegment
    from late_arriving_demo.raw.raw_customer_asia
)

select * from customers