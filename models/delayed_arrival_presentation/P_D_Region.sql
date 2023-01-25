--This is an example of a dimension that might have records arrive after the fact orders so these records are collected as unknowns.  Please note this is currently type 1 but could be changed to type 2.

{{config(
        materialized = 'table'
        ,tags = ["dimension","region"]
        ,schema='PRES'
        )
}}

With cte_c_region
as
(
    select
    r_regionkey as D_REGION_NK
    ,r_name as D_REGION_NAME
    from {{ref('Region')}}
)
,cte_c_order_late_arriving_region
as
(
    Select 
    region_nk as D_REGION_NK
    from {{ref('C_L2_Orders')}} co
    where Not exists
    (
        Select
        D_REGION_NK
        from cte_c_region ccr
        where co.region_nk = ccr.D_REGION_NK

    )
)
,cte_d_region
as
(
  Select
  D_REGION_NK
  ,'Unknown' as D_REGION_NAME
  from cte_c_order_late_arriving_region
    
  Union All
  
  Select
  D_REGION_NK
  ,D_REGION_NAME
  from cte_c_region
)
    
Select 
md5(D_REGION_NK) as D_REGION_SK
,* 
,current_timestamp as audit_datetime
from cte_d_region