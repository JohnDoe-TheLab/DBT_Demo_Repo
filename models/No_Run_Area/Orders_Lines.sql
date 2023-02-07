{{ config(schema='CUR') }}

With Orders_Lines_America as 
(
    Select
    l_linekey
    ,l_orderkey
    ,l_amount
    ,l_linetypekey
    ,l_create_datetime
    ,l_update_datetime
    ,cdc_operation
    ,DELETE_FLAG
    ,AUDIT_DATETIME
    From LATE_ARRIVING_DEMO.RAW.RAW_ORDERS_LINES_AMERICA
),
Orders_Lines_Asia as 
(
    Select
    l_linekey
    ,l_orderkey
    ,l_amount
    ,l_linetypekey
    ,l_create_datetime
    ,l_update_datetime
    ,cdc_operation
    ,DELETE_FLAG
    ,AUDIT_DATETIME
    From LATE_ARRIVING_DEMO.RAW.RAW_ORDERS_LINES_ASIA
)

Select
*
from Orders_Lines_America

Union All

Select
*
from Orders_Lines_Asia