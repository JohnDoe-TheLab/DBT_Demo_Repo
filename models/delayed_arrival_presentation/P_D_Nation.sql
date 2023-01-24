--This is an example of a dimension that might have records arrive after the fact orders so these records are collected as unknowns.  Please note this is currently type 1 but could be changed to type 2.
--This dimension also contains heirarchy info for region
{{config(
        materialized = 'table'
        ,tags = ["dimension","region","nation"]
        ,schema='PRES'
        )
}}

with cte_c_nation
as
(
    select
    n_nationkey as D_NATION_NK
    ,n_name as D_NATION_NAME
    ,pr.d_region_sk as D_REGION_SK
    from {{ref('Nation')}} cn
    left join {{ref('P_D_Region')}} pr
    on cn.n_regionkey = pr.d_region_nk
)
,cte_c_order_late_arriving_nation
as
(
    Select 
    nation_nk as D_NATION_NK
    from {{ref('C_L2_Orders')}} co
    where Not exists
    (
        Select
        D_NATION_NK
        from cte_c_nation ccn
        where co.nation_nk = ccn.D_NATION_NK

    )
)
,cte_d_nation
as
(
  Select
  D_NATION_NK
  ,'Unknown' as D_NATION_NAME
  ,'-1' as D_REGION_SK
  from cte_c_order_late_arriving_nation
    
  Union All
  
  Select
  D_NATION_NK
  ,D_NATION_NAME
  ,D_REGION_SK  
  from cte_c_nation
)
    
Select 
md5(D_NATION_NK) as D_NATION_SK
,* 
,current_timestamp as audit_datetime
from cte_d_nation