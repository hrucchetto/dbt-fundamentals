with total_revenues as (
    select sum(amount) total_revenues
    from {{ ref('stg_payments') }}
    where status = 'success'
)

select * from total_revenues