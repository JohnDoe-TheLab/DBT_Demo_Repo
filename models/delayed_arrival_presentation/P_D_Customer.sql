--This is an example of a dimension that might have records arrive after the fact orders so these records are collected as unknowns.  Please note this is currently type 1 but could be changed to type 2.

{{config(
        materialized = 'table'
        ,tags = ["dimension","customer"]
        ,schema='PRES'
        )
}}

With cte_c_customer
as
(
    Select
    c_custkey as D_CUSTKEY_NK
    ,c_name as D_NAME
    ,c_address as D_ADDRESS
    ,c_acctbal as D_ACCTBAL
    ,c_mktsegment as D_MKTSEGMENT
    from {{ref('Customers')}} cc
),
cte_c_order_late_arriving_customer
as
(
    Select 
    cust_nk as D_CUSTKEY_NK
    from {{ref('C_L2_Orders')}} co
    where Not exists
    (
        Select
        D_CUSTKEY_NK
        from cte_c_customer ccc
        where co.cust_nk = ccc.D_CUSTKEY_NK

    )
)
,cte_d_customer
as
(
  Select
  D_CUSTKEY_NK
  ,'Unknown' as D_NAME
  ,'Unknown' as D_ADDRESS
  ,null as D_ACCTBAL
  ,'Unknown' as D_MKTSEGMENT
  from cte_c_order_late_arriving_customer
    
  Union All
  
  Select
  D_CUSTKEY_NK
  ,D_NAME
  ,D_ADDRESS
  ,D_ACCTBAL
  ,D_MKTSEGMENT
  from cte_c_customer
)
    
Select 
md5(D_CUSTKEY_NK) as D_CUST_SK
,* 
,current_timestamp as audit_datetime
from cte_d_customer