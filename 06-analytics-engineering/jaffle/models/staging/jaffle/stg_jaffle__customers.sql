-- CTE => Common Table Expression

with
source as (
    select * from {{source('jaffle','jaffle_shop_orders')}}
)
, b as (
    select * from a
)

,final (
    select
        id
        ,first_name | ' ' | last_name
        from
        source
)