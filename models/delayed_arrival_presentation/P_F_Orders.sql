{{config(
        materialized = 'incremental'
        ,tags = ["curation","orders","lines"]
        ,unique_key = 'order_nk'
        ,schema='PRES'
        )
}}

With cte_cl2_orders
as
(
    Select
    order_nk
    ,cust_nk
    ,nation_nk
    ,region_nk
    ,d_date_sid
    ,order_price
    ,order_line_count
    from {{ref('C_L2_Orders')}} co
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
     where AUDIT_DATETIME > (select max(AUDIT_DATETIME) from {{ this }})

    {% endif %}
)
,cte_p_dcust
as
(
    Select
    d_cust_sk
    ,d_custkey_nk
    from {{ref('P_D_Customer')}} pdc
    where exists
    (
        Select
        cust_nk
        From cte_cl2_orders cco
        where pdc.d_custkey_nk = cco.cust_nk
    )
)
,cte_p_dregion
as
(
    Select
    d_region_sk
    ,d_region_nk
    from {{ref('P_D_Region')}} pdr
    where exists
    (
        Select
        region_nk
        From cte_cl2_orders cco
        where pdr.d_region_nk = cco.region_nk
    )
)
,cte_p_dnation
as
(
    Select
    d_nation_sk
    ,d_nation_nk
    from {{ref('P_D_Nation')}} pdn
    where exists
    (
        Select
        nation_nk
        From cte_cl2_orders cco
        where pdn.d_nation_nk = cco.nation_nk
    )
)
,cte_f_orders
as
(
  Select
  order_nk
  ,d_cust_sk
  ,d_nation_sk
  ,d_region_sk
  ,d_date_sid
  ,Order_price
  ,order_line_count
  from cte_cl2_orders cco
  Inner Join cte_p_dcust cpdc
    On cco.cust_nk=cpdc.d_custkey_nk
  Inner Join cte_p_dnation cpdn
    On cco.nation_nk=cpdn.d_nation_nk
  Inner Join cte_p_dregion cpdr
    On cco.region_nk=cpdr.d_region_nk
)

Select
md5(order_nk) as F_CUST_SK
,* 
,current_timestamp as audit_datetime
from cte_f_orders