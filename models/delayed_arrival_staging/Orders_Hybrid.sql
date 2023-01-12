{{ config(schema='CUR') }}


With DELTA_AMERICA as 
    (
        Select  
            AUDIT_DATETIME
            ,DELETE_FLAG
            ,O_ORDERKEY
            ,O_CUSTKEY
            ,O_NATIONKEY
            ,O_REGIONKEY
            ,O_TOTALPRICE
            ,O_ORDERDATE
            ,O_CREATE_DATETIME
            ,O_UPDATE_DATETIME
        FROM LATE_ARRIVING_DEMO.RAW.RAW_ORDERS_AMERICA_DELTA
        changes(information => append_only)
        at(stream => 'LATE_ARRIVING_DEMO.RAW.STM_ORDERS_AMERICA_DELTA')
        QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY O_ORDERKEY ORDER BY AUDIT_DATETIME DESC)
    ),
    HYBRID_AMERICA as
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
     WHERE NOT EXISTS
        (
          SELECT 1
          FROM DELTA_AMERICA D
          WHERE D.O_ORDERKEY = B.O_ORDERKEY
        )
     UNION ALL
      Select 
          D.AUDIT_DATETIME
          ,DELETE_FLAG
          ,D.O_ORDERKEY
          ,O_CUSTKEY
          ,O_NATIONKEY
          ,O_REGIONKEY
          ,D.O_TOTALPRICE
          ,D.O_ORDERDATE
          ,D.O_CREATE_DATETIME
          ,D.O_UPDATE_DATETIME
      FROM DELTA_AMERICA D
      ),
    DELTA_ASIA as 
    (
        Select  
            AUDIT_DATETIME
            ,DELETE_FLAG
            ,O_ORDERKEY
            ,O_CUSTKEY
            ,O_NATIONKEY
            ,O_REGIONKEY
            ,O_TOTALPRICE
            ,O_ORDERDATE
            ,O_CREATE_DATETIME
            ,O_UPDATE_DATETIME
        FROM LATE_ARRIVING_DEMO.RAW.RAW_ORDERS_ASIA_DELTA
        changes(information => append_only)
        at(stream => 'LATE_ARRIVING_DEMO.RAW.STM_ORDERS_ASIA_DELTA')
        QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY O_ORDERKEY ORDER BY AUDIT_DATETIME DESC)
    ),
    HYBRID_ASIA as
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
     WHERE NOT EXISTS
        (
          SELECT 1
          FROM DELTA_ASIA D
          WHERE D.O_ORDERKEY = B.O_ORDERKEY
        )
     UNION ALL
      Select 
          D.AUDIT_DATETIME
          ,DELETE_FLAG
          ,D.O_ORDERKEY
          ,O_CUSTKEY
          ,O_NATIONKEY
          ,O_REGIONKEY
          ,D.O_TOTALPRICE
          ,D.O_ORDERDATE
          ,D.O_CREATE_DATETIME
          ,D.O_UPDATE_DATETIME
      FROM DELTA_ASIA D
      )
      
      Select
      * 
      from hybrid_america
      
      Union All
      
      Select
      *
      from hybrid_asia