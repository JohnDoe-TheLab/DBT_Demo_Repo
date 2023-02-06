{{ config(schema='CUR') }}

With LineItems_America as 
(
    Select
    l_orderkey
    ,l_partkey
    ,l_suppkey
    ,l_linenumber
    ,l_quantity
    ,l_extendedprice
    ,l_discount
    ,l_tax
    ,l_returnflag
    ,l_shipdate
    ,l_commitdate
    ,l_receiptdate
    ,l_shipmode
    ,l_comment
    ,cdc_operation
    ,DELETE_FLAG
    ,AUDIT_DATETIME
    From LATE_ARRIVING_DEMO.RAW.RAW_LINEITEMS_AMERICA
),
LineItems_Asia as 
(
    Select
    l_orderkey
    ,l_partkey
    ,l_suppkey
    ,l_linenumber
    ,l_quantity
    ,l_extendedprice
    ,l_discount
    ,l_tax
    ,l_returnflag
    ,l_shipdate
    ,l_commitdate
    ,l_receiptdate
    ,l_shipmode
    ,l_comment
    ,cdc_operation
    ,DELETE_FLAG
    ,AUDIT_DATETIME
    From LATE_ARRIVING_DEMO.RAW.RAW_LINEITEMS_ASIA
)

Select
*
from LineItems_America

Union All

Select
*
from LineItems_Asia