{{config(
        materialized = 'table'
        ,tags = ["curation","orders","lines"]
        ,unique_key = 'cust_nk'
        ,schema='PRES'
        )
}}

With cte_orders_lines as
(
    Select 
    l_orderkey
    ,count(l_linekey) as Line_Count
    from {{ref('C_L1_Order_Lines')}}
    group by
    l_orderkey
)
,cte_orders as
(
    Select
    o_orderkey
    ,o_custkey
    ,o_nationkey
    ,o_regionkey
    ,replace(to_char(o_orderdate),'-','') as d_date_sid
    ,o_totalprice
    from {{ref('C_L1_Order')}} o
    where exists
    (
        Select
        l_orderkey
        from {{ref('C_L1_Order_Lines')}} ol
        where ol.l_orderkey = o.o_orderkey
    )
)
,cte_orders_agg_logic as
(
    Select 
    TO_NUMERIC(concat(to_char(h.o_custkey),to_char(h.o_nationkey),to_char(h.o_regionkey),to_char(h.d_date_sid)),38,0) as order_nk
    ,h.o_custkey as cust_nk
    ,h.o_nationkey as nation_nk
    ,h.o_regionkey as region_nk
    ,h.d_date_sid
    ,sum(h.o_totalprice) as Order_Price
    ,sum(l.line_count) as Order_Line_Count
    from cte_orders h
    Inner Join cte_orders_lines l
    On h.o_orderkey = l.l_orderkey
    Group by
    h.o_custkey
    ,h.o_nationkey
    ,h.o_regionkey
    ,h.d_date_sid
)

Select
*
from cte_orders_agg_logic