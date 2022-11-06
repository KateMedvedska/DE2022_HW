delete from `{{ params.project_id }}.silver.sales`
where purchase_date = date("{{ ds }}")
;

insert `{{ params.project_id }}.silver.sales` (
    client_id,
    purchase_date,
    product_name,
    price,
    _id,
    _logical_dt,
    _job_start_dt
)
select
    CustomerId as client_id,
    coalesce(safe_cast(PurchaseDate as date), parse_date('%Y/%m/%d', PurchaseDate)) as purchase_date,
    Product as product_name,
    cast(rtrim(Price, '$') as integer) as price,
    _id,
    _logical_dt,
    _job_start_dt
from `{{ params.project_id }}.bronze.sales`
where date(_logical_dt) = date("{{ ds }}")
        and coalesce(safe_cast(PurchaseDate as date), parse_date('%Y/%m/%d', PurchaseDate)) = date("{{ ds }}")
;