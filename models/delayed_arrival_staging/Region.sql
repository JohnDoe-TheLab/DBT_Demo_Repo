{{ config(schema='CUR') }}

with regions as
(
    Select
    r_regionkey
    ,r_name
    from
    late_arriving_demo.raw.raw_region_america

    Union All

    Select
    r_regionkey
    ,r_name
    from
    late_arriving_demo.raw.raw_region_asia
)

Select * from regions