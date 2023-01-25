{{ config(schema='CUR') }}

        With Orders_Delta_America as
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

        Orders_Delta_Asia as
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
        )

        Select
        *
        from Orders_Delta_America

        Union All

        Select
        *
        from Orders_Delta_Asia
