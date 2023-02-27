{{ config(schema='CUR') }}

with nations as
(
    Select
    n_nationkey
    ,n_name
    ,n_regionkey
    from dbt_data_eng_demo.raw.raw_nation_america

    Union All

    Select
    n_nationkey
    ,n_name
    ,n_regionkey
    from dbt_data_eng_demo.raw.raw_nation_asia
)

Select * from nations