{{
  config(
    materialized='view'
    ,schema='CUR'
  )
}}


    With ORDERS_AMERICA as
    (
      select 
      B.AUDIT_DATETIME
      ,DELETE_FLAG
      ,B.O_ORDERKEY
      ,O_CUSTKEY
      ,O_NATIONKEY
      ,O_REGIONKEY
      ,B.O_TOTALPRICE
      ,B.O_ORDERDATE
      ,B.O_CREATE_DATETIME
      ,B.O_UPDATE_DATETIME
     FROM LATE_ARRIVING_DEMO.RAW.RAW_ORDERS_AMERICA B --This is the base table
    ), 
    ORDERS_ASIA as
    (
      select 
      B.AUDIT_DATETIME
      ,DELETE_FLAG
      ,B.O_ORDERKEY
      ,O_CUSTKEY
      ,O_NATIONKEY
      ,O_REGIONKEY
      ,B.O_TOTALPRICE
      ,B.O_ORDERDATE
      ,B.O_CREATE_DATETIME
      ,B.O_UPDATE_DATETIME
     FROM LATE_ARRIVING_DEMO.RAW.RAW_ORDERS_ASIA B --This is the base table
      )
      
      Select
      * 
      from orders_america
      
      Union All
      
      Select
      *
      from orders_asia